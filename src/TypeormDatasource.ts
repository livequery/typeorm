import { FindManyOptions, In, LessThan, LessThanOrEqual, Like, MoreThan, MoreThanOrEqual, Not, Repository, Connection, Between } from "typeorm"
import { UpdatedData, QueryData, LivequeryRequest } from '@livequery/types'
import { Subject, fromEvent } from "rxjs"
import { filter, mergeMap } from 'rxjs/operators'
import { Cursor } from "./Cursor"
import createPostgresSubscriber, { Subscriber } from "pg-listen"
import { CreateTableTriggerSqlBuilder } from "./sql/CreateTableTriggerSqlBuilder"
import { PostgresConnectionOptions } from "typeorm/driver/postgres/PostgresConnectionOptions"
import { CreateUpdateListenerSqlBuilder } from "./sql/CreateUpdateListenerSqlBuilder"
import { PostgresDataChangePayload } from "./PostgresDataChangePayload"


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

    async query(query: LivequeryRequest): Promise<QueryData> {

        const { ref, is_collection, schema_collection_ref, options, keys, filters } = query

        const query_params: FindManyOptions = {
            where: {
                ...keys
            },
            take: options._limit + 1,
            order: {
                created_at: 'DESC',
                ...options._order_by ? {
                    [options._order_by]: options._sort?.toUpperCase() as 1 | "DESC" | "ASC" | -1
                } : {},
            },

            ...options._select ? { select: (options as any)._select as string[] } : {},
        }

        const mapper = {
            '==': (key: string, value: any) => query_params.where[key] = value,
            '!=': (key: string, value: any) => query_params.where[key] = Not(value),
            '<': (key: string, value: any) => query_params.where[key] = LessThan(value),
            '<=': (key: string, value: any) => query_params.where[key] = LessThanOrEqual(value),
            '>': (key: string, value: any) => query_params.where[key] = MoreThan(value),
            '>=': (key: string, value: any) => query_params.where[key] = MoreThanOrEqual(value),
            'contains': (key: string, value: any) => query_params.where[key] = In(value),
            'not-contains': (key: string, value: any) => query_params.where[key] = Not(In(value)),
            like: (key: string, value: any) => query_params.where[key] = Like(value),
            between: (key: string, [a, b]: [number, number]) => query_params.where[key] = Between(a, b)
        }



        for (const [key, expression, value] of filters) {
            const fn = mapper[expression as string]
            fn && fn(key, value)
        }


        if (options._cursor) {
            const value = Cursor.decode(options._cursor || Date.now())
            query_params.where[options._order_by as string || 'created_at'] = options._sort == 'asc' ? MoreThan(value) : LessThan(value)
        }

        const repository = this.refs_map.get(schema_collection_ref)
        if (!repository) throw { code: 'REF_NOT_FOUND', ref, schema_collection_ref }
        const conditions = { ...query_params, ...keys }

        if (!is_collection) {
            const data = await repository.findOne(conditions) ?? null 
            return { data }
        }

        const data = await repository.find(conditions)
        const has_more = data.length > options._limit
        const items = data.slice(0, options._limit)
        const next_cursor = has_more ? Cursor.encode(items[options._limit - 1][options._order_by as string || 'created_at']) : null

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

        const subscribers = new Map<Connection, {
            subscriber: Subscriber,
            tables: Map<string, {
                function_name: string,
                repository: Repository<any>
            }>
        }>()

        // Init subscribers
        for (const repository of new Set([...this.refs_map.values()])) {

            const connection = repository.metadata.connection

            if (!subscribers.has(connection)) {
                const { database, host, port, username, password } = connection.options as PostgresConnectionOptions
                const subscriber = createPostgresSubscriber({ host, port, user: username, password, database })
                await subscriber.connect()
                subscribers.set(connection, {
                    subscriber,
                    tables: new Map()
                })
            }

            const { tables } = subscribers.get(connection)

            const table_name = repository.metadata.tableName
            const function_name = `listen_update_for_${table_name.replaceAll('-', '_')}`
            if (!tables.has(table_name)) {
                tables.set(table_name, { function_name, repository })
            }
        }

        // Active listeners
        for (const [connection, { subscriber, tables }] of subscribers) {

            for (const [table_name, { function_name, repository }] of tables) {

                const CreateUpdateListenerCMD = CreateUpdateListenerSqlBuilder(function_name, [... this.repositories_map.get(repository)])
                await repository.query(CreateUpdateListenerCMD)

                const CreateTableTriggerCMD = CreateTableTriggerSqlBuilder(table_name, function_name)
                await repository.query(CreateTableTriggerCMD)


            }
            await subscriber.listenTo('realtime_sync')
            fromEvent<PostgresDataChangePayload>(subscriber.notifications, 'realtime_sync')
                .pipe(
                    filter(payload => payload.type == 'removed' || !!payload.data),
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


        }

    }

}