import { FindManyOptions, getConnection, In, LessThan, LessThanOrEqual, Like, MoreThan, MoreThanOrEqual, Not, Repository, getConnectionOptions, Connection } from "typeorm"
import { QueryOption, UpdatedData, QueryData } from '@livequery/types'
import { Subject, fromEvent } from "rxjs"
import { filter, mergeMap, tap } from 'rxjs/operators'
import { Cursor } from "./Cursor"
import createPostgresSubscriber, { Subscriber } from "pg-listen"
import { PostgresDataChangePayload } from "./PostgresDataChangePayload"
import { CreateTableTriggerSqlBuilder } from "./CreateTableTriggerSqlBuilder"
import { PostgresConnectionOptions } from "typeorm/driver/postgres/PostgresConnectionOptions"
import { CreateUpdateListenerSqlBuilder } from "./CreateUpdateListenerSqlBuilder"



export class TypeormDatasource {

    readonly changes = new Subject<UpdatedData<any>>()

    private refs_map = new Map<string, Repository<any>>()
    private repositories_map = new Map<Repository<any>, Set<string>>()

    constructor(list: Array<{ repository: Repository<any>, ref: string }> = []) {

        for (const { ref, repository } of list) {
            const full_collection_ref = ref.replaceAll(':', '')
            const short_collection_ref = full_collection_ref.split('/').filter((r, i) => i % 2 == 0).join('/')
            this.refs_map.set(short_collection_ref, repository)

            if (!this.repositories_map.has(repository)) this.repositories_map.set(repository, new Set())
            this.repositories_map.get(repository).add(full_collection_ref)
        }
    }

    async query(ref: string, keys: object, options: QueryOption<any>): Promise<QueryData> {

        const query_params: FindManyOptions = {
            where: {
                ...keys
            },
            take: options._limit + 1,
            ...options._order_by ? {
                order: {
                    created_at: 'DESC',
                    [options._order_by]: options._sort?.toUpperCase() as 1 | "DESC" | "ASC" | -1
                }
            } : {},
            ...options._select ? { select: (options as any)._select as string[] } : {},
        }

        const mapper = {
            eq: (key: string, value: any) => query_params.where[key] = value,
            ne: (key: string, value: any) => query_params.where[key] = Not(value),
            lt: (key: string, value: any) => query_params.where[key] = LessThan(value),
            lte: (key: string, value: any) => query_params.where[key] = LessThanOrEqual(value),
            gt: (key: string, value: any) => query_params.where[key] = MoreThan(value),
            gte: (key: string, value: any) => query_params.where[key] = MoreThanOrEqual(value),
            'in-array': (key: string, value: any) => query_params.where[key] = In(value),
            'not-in-array': (key: string, value: any) => query_params.where[key] = Not(In(value)),
            like: (key: string, value: any) => query_params.where[key] = Like(value),

        }

        for (const [expression, key, value] of options.filters) {
            mapper[expression as string] && mapper[expression as string](key, value)
        }


        if (options._cursor) {
            const value = Cursor.decode(options._cursor)
            query_params.where[options._order_by as string] = options._sort == 'asc' ? MoreThan(value) : LessThan(value)
        }

        const refs = ref.split('/')
        const is_collection = refs.length % 2 == 1
        const collection_ref = refs.filter((_, i) => i % 2 == 0).join('/')
        const repository = this.refs_map.get(collection_ref)
        if (!repository) throw { code: 'REF_NOT_FOUND', ref, collection_ref }
        const filters = { ...query_params, ...keys }
        const data = is_collection ? await repository.find(filters) : [await repository.findOne(filters)]

        const has_more = data.length > options._limit
        const items = data.slice(0, options._limit)
        const next_cursor = has_more ? Cursor.encode(items[options._limit - 1][options._order_by as string]) : null

        return {
            data: {
                items,
                paging: { has_more, next_cursor }
            }
        }
    }

    private postgres_listening = false
    async active_postgres_sync() {
        if (this.postgres_listening) return
        this.postgres_listening = true

        const subscribers = new Map<Connection, Subscriber>()

        const repositories = new Set([...this.refs_map.values()])
        for (const repository of repositories) {

            // Get connection
            const connection = repository.metadata.connection

            const { database, host, port, username, password } = connection.options as PostgresConnectionOptions
            const subscriber = subscribers.get(connection) || createPostgresSubscriber({ host, port, user: username, password, database })
            subscribers.set(connection, subscriber)

            // Connect 
            await subscriber.connect()

            // Setup 
            await subscriber.listenTo('realtime_update')
            fromEvent<PostgresDataChangePayload<any>>(subscriber.notifications, 'realtime_update')
                .pipe(
                    filter(payload => !!payload.data),
                    mergeMap(({ refs, type, id, data: updated_values = {}, doc }) => {
                        const data = { id, ...updated_values || {} }
                        if (type == 'added' || type == 'removed') return refs.map(({ ref }) => ({ ref, data, type }))
                        if (type == 'modified') return refs.reduce((p, { old_ref, ref }) => {
                            if (old_ref == ref) {
                                p.push({ data, ref, type })
                            } else {
                                p.push({ data: { id: doc.id }, ref: old_ref, type: 'removed' })
                                p.push({ data: doc, ref, type: 'added' })
                            }
                            return p
                        }, [] as UpdatedData[])
                        return []
                    })
                )
                .subscribe(this.changes)

            // Create trigger
            const table_name = repository.metadata.tableName
            await connection.query(CreateUpdateListenerSqlBuilder(table_name, [... this.repositories_map.get(repository)]))
            repository.query(CreateTableTriggerSqlBuilder(table_name))

        }


    }

}