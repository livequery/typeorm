import { FindManyOptions, getConnection, In, LessThan, LessThanOrEqual, Like, MoreThan, MoreThanOrEqual, Not, Repository } from "typeorm"
import { QueryOption, UpdatedData } from '@livequery/types'
import { Subject, fromEvent } from "rxjs"
import { filter, mergeMap } from 'rxjs/operators'
import { Cursor } from "./Cursor"
import createPostgresSubscriber from "pg-listen"
import { PostgresDataChangePayload } from "./PostgresDataChangePayload"
import { CreateTableTrigger } from "./CreateTableTrigger"
import { waitFor } from "./waitFor"



export class TypeormDatasource {

    private readonly changes = new Subject<UpdatedData<any>>()

    private refs_map = new Map<string, Repository<any>>()
    private tables_map = new Map<string, Set<string>>()

    constructor(list: Array<{ repository: Repository<any>, refs: string[] }>) {

        for (const { refs, repository } of list) {
            const table_name = repository.metadata.tableName
            if (this.tables_map.has(table_name)) this.tables_map.set(table_name, new Set())

            for (const ref of refs) {
                const full_collection_ref = ref.replaceAll(':', '')
                this.tables_map.get(table_name).add(full_collection_ref)

                const short_collection_ref = full_collection_ref.split('/').filter((r, i) => i % 2 == 1).join('/')
                this.refs_map.set(short_collection_ref, repository)
            }
        }
    }

    async query(ref: string, keys: object, options: QueryOption<any>) {
        const query_params: FindManyOptions = {
            where: {
                created_at: options._sort == 'asc' ? MoreThan(0) : LessThan(Date.now())
            },
            take: options._limit + 1,
            order: {
                created_at: 'DESC',
                [options._order_by]: options._sort.toUpperCase() as 1 | "DESC" | "ASC" | -1
            },
            select: (options as any)._select as string[],
        }

        const mapper = {
            eq: (key: string, value: any) => query_params.where[key] = value,
            like: (key: string, value: any) => query_params.where[key] = Like(value),
            ne: (key: string, value: any) => query_params.where[key] = Not(value),
            lt: (key: string, value: any) => query_params.where[key] = LessThan(value),
            lte: (key: string, value: any) => query_params.where[key] = LessThanOrEqual(value),
            gt: (key: string, value: any) => query_params.where[key] = MoreThan(value),
            gte: (key: string, value: any) => query_params.where[key] = MoreThanOrEqual(value),
            in: (key: string, value: any) => query_params.where[key] = In(value),
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
        const collection_ref = refs.filter((_, i) => i % 2 == 1).join('/')
        const repository = this.refs_map.get(collection_ref)
        if (!repository) throw { code: 'REF_NOT_FOUND', ref, collection_ref }
        const filters = { ...query_params, ...keys }
        const data = is_collection ? await repository.find(filters) : [await repository.findOne(filters)]

        const has_more = data.length > options._limit
        const items = data.slice(0, options._limit)
        const cursor = has_more ? Cursor.encode(items[options._limit - 1][options._order_by as string]) : null

        return {
            data: {
                items,
                count: items.length,
                has_more,
                cursor
            }
        }
    }

    private postgres_listening = false
    async active_postgres_sync() {
        if (this.postgres_listening) return
        this.postgres_listening = true

        // Create listener  
        const { options: { database, host, port, username, password } } = await waitFor(getConnection) as any
        const subscriber = createPostgresSubscriber({ host, port, user: username, password, database })
        await subscriber.connect()
        await subscriber.listenTo('realtime_update')
        fromEvent<PostgresDataChangePayload<any>>(subscriber.notifications, 'realtime_update')
            .pipe(
                filter(payload => !!payload.data && this.tables_map.has(payload.table)),
                mergeMap(payload => {
                    const refs = this.tables_map.get(payload.table)
                    return [...refs].map(paths => {
                        const doc = ({ ...payload.new || {}, ...payload.old || {} })
                        const ref = paths.split('/').map((key, i) => i % 2 == 1 ? key : doc[key]).join('/')
                        return { ...payload.data, ref }
                    })
                })
            )
            .subscribe(this.changes)

        // Create trigger
        for (const [ref, { repository }] of Object.entries(this.tables_map)) {
            const table_name = repository.metadata.tableName
            const script = CreateTableTrigger.replaceAll('__TABLE__', table_name)
            repository.query(script)
        }
    }

}