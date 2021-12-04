import { LivequeryRequest, UpdatedData } from '@livequery/types'
import createPostgresSubscriber, { Subscriber } from "pg-listen"
import { fromEvent, Subject } from "rxjs"
import { filter, mergeMap, map, tap } from 'rxjs/operators'
import { Between, Connection, EntityTarget, FindManyOptions, getConnectionManager, getMetadataArgsStorage, getRepository, ILike, LessThan, LessThanOrEqual, MoreThan, MoreThanOrEqual, Not, Repository } from "typeorm"
import { Cursor } from "./helpers/Cursor"
import { DataChangePayload } from "./DataChangePayload"
import { CreateTableTriggerSqlBuilder } from "./sql/CreateTableTriggerSqlBuilder"
import { CreateUpdateListenerSqlBuilder } from "./sql/CreateUpdateListenerSqlBuilder"
import { v4 } from 'uuid'
import { JsonUtil } from './helpers/JsonUtil'


export class TypeormDatasource {

    public readonly changes = new Subject<UpdatedData<any>>()
    #refs_map = new Map<string, Repository<any>>()// Short schema => table for query
    #repositories_map = new Map<Repository<any>, Set<string>>() // Full table => full uri key schema for sync


    async register<T = any>(schema_ref: string, entity: EntityTarget<T>, connection: string = 'default') {

        if (schema_ref.match(/[^\-_a-zA-Z0-9\/:]+/)) throw new Error(
            `Don't use special chars in livequery ref "${schema_ref}"`
        )

        const collection_ref = schema_ref.split('/').filter((_, i) => i % 2 == 0).join('/')

        // Load repository
        const repository = await getRepository(entity, connection)

        // Add to paths
        this.#refs_map.set(schema_ref, repository)

        // Add to repo
        !this.#repositories_map.has(repository) && this.#repositories_map.set(repository, new Set())
        this.#repositories_map.get(repository).add(collection_ref)
    }

    async query(query: LivequeryRequest) {

        const repository = this.#refs_map.get(query.schema_collection_ref)
        if (!repository) throw { code: 'REF_NOT_FOUND', message: 'Missing ref config in livequery system' }


        if (query.method == 'get') return this.#excute_get(repository, query)
        if (query.method == 'post') return this.#excute_post(repository, query)
        if (query.method == 'put') return this.#excute_put(repository, query)
        if (query.method == 'patch') return this.#excute_patch(repository, query)
        if (query.method == 'del') return this.#excute_del(repository, query)
    }

    async #excute_get(repository: Repository<any>, query: LivequeryRequest) {
        const { is_collection, options, keys, filters } = query

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
            'eq': (key: string, value: any) => query_params.where[key] = value,
            'ne': (key: string, value: any) => query_params.where[key] = Not(value),
            'lt': (key: string, value: any) => query_params.where[key] = LessThan(value),
            'lte': (key: string, value: any) => query_params.where[key] = LessThanOrEqual(value),
            'gt': (key: string, value: any) => query_params.where[key] = MoreThan(value),
            'gte': (key: string, value: any) => query_params.where[key] = MoreThanOrEqual(value),
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



        // Add like expression
        const like_expressions = filters.filter(f => f[1] == 'like')
        if (like_expressions.length > 0) {
            query_params.where = like_expressions.map(([key, _, value]) => ({
                [key]: ILike(value),
                ...query_params.where as any
            }))
        }

        // Document query
        if (!is_collection) {
            const data = await repository.findOne(query_params) ?? null
            return data
        }

        // Collection query
        const data = await repository.find(query_params)
        const has_more = data.length > options._limit
        const items = data.slice(0, options._limit)
        const next_cursor = has_more ? Cursor.encode(items[options._limit - 1][options._order_by as string || 'created_at']) : null

        return {
            items,
            paging: { has_more, next_cursor }
        }
    }

    async #excute_post(repository: Repository<any>, query: LivequeryRequest) {
        const data = { id: v4(), ...query.body }
        return await repository.save(data)
    }

    async #excute_put(repository: Repository<any>, query: LivequeryRequest) {
        return await repository.update(query.keys, query.body)
    }

    async #excute_patch(repository: Repository<any>, query: LivequeryRequest) {
        return await repository.update(query.keys, query.body)
    }

    async #excute_del(repository: Repository<any>, query: LivequeryRequest) {
        return await repository.delete(query.keys)
    }

    async active_postgres_sync() {

        const subscribers = new Map<Connection, {
            subscriber: Subscriber,
            tables: Map<string, {
                function_name: string,
                repository: Repository<any>
            }>
        }>()

        // Init subscribers
        for (const repository of new Set([...this.#refs_map.values()])) {

            const connection = repository.metadata.connection

            if (!subscribers.has(connection)) {
                const { database, host, port, username, password } = connection.options as any
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
        for (const [_, { subscriber, tables }] of subscribers) {

            for (const [table_name, { function_name, repository }] of tables) {

                const refs = [...this.#repositories_map.get(repository).values()]
                const CreateUpdateListenerCMD = CreateUpdateListenerSqlBuilder(function_name, refs)
                await repository.query(CreateUpdateListenerCMD)
                const CreateTableTriggerCMD = CreateTableTriggerSqlBuilder(table_name, function_name)
                await repository.query(CreateTableTriggerCMD)
            }
            await subscriber.listenTo('realtime_sync')
            fromEvent<DataChangePayload>(subscriber.notifications, 'realtime_sync')
                .pipe(
                    filter(payload => payload.type == 'removed' || !!payload.data),
                    mergeMap(({ refs, type, id, data: updated_values = {}, new_doc }) => {
                        const data = JsonUtil.deepJsonParse({ id, ...updated_values || {} })
                        if (type == 'added' || type == 'removed') return refs.map(({ ref }) => ({ ref, data, type }))
                        if (type == 'modified') return refs.reduce((p, { old_ref, ref }) => {
                            if (old_ref == ref) {
                                p.push({ data, ref, type })
                            } else {
                                p.push({ data: { id: new_doc.id }, ref: old_ref, type: 'removed' })
                                p.push({ data: new_doc, ref, type: 'added' })
                            }
                            return p
                        }, [] as UpdatedData[])
                        return [] as UpdatedData[]
                    })
                )
                .subscribe(this.changes)
        }

    }

}

