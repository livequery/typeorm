import { Between, DataSource, FindManyOptions, ILike, LessThan, LessThanOrEqual, MoreThan, MoreThanOrEqual, Not, Repository } from "typeorm";
import { LivequeryRequest, UpdatedData } from '@livequery/types'
import { filter, fromEvent, mergeMap, Subject } from "rxjs";
import { DataChangePayload } from "./DataChangePayload";
import { JsonUtil } from "./helpers/JsonUtil";
import { CreateTableTriggerSqlBuilder } from "./sql/CreateTableTriggerSqlBuilder";
import { CreateUpdateListenerSqlBuilder } from "./sql/CreateUpdateListenerSqlBuilder";
import { Cursor } from './helpers/Cursor'
import { v4 } from 'uuid'
import createPostgresSubscriber, { Subscriber } from "pg-listen"
import { RouteOptions } from "./RouteOptions";




export class TypeormDatasource {

    #refs_map = new Map<string, Repository<any>>()
    #repositories_map = new Map<Repository<any>, Set<string>>()
    #entities_map = new Map<any, Repository<any>>()
    #realtime_repositories: Repository<any>[] = []
    public readonly changes = new Subject()

    #init_progress: Promise<void>

    constructor(
        private connections: { [key: string]: DataSource },
        private config: Array<RouteOptions & { refs: string[] }>
    ) {
        this.#init_progress = this.#init()
    }

    async #init() {

        for (const { connection = 'default', entity, refs, realtime = false } of this.config) {
            for (const ref of refs) {
                const datasource = this.connections[connection]
                if (!datasource) throw new Error(`Can not find [${connection}] datasource`)

                const repository = this.#entities_map.get(entity) ?? await datasource.getRepository(entity)
                this.#entities_map.set(entity, repository)

                const schema_ref = ref.replaceAll(':', '')
                this.#repositories_map.set(repository, new Set([
                    ... (this.#repositories_map.get(repository) || []),
                    schema_ref
                ]))
                this.#refs_map.set(schema_ref, repository)
                realtime && this.#realtime_repositories.push(repository)
            }
        }
    }


    async query(query: LivequeryRequest) {
        await this.#init_progress

        const repository = this.#refs_map.get((query as any).schema_ref)
        if (!repository) throw { code: 'REF_NOT_FOUND', message: 'Missing ref config in livequery system' }


        if (query.method == 'get') return this.#get(repository, query)
        if (query.method == 'post') return this.#post(repository, query)
        if (query.method == 'put') return this.#put(repository, query)
        if (query.method == 'patch') return this.#patch(repository, query)
        if (query.method == 'delete') return this.#del(repository, query)
    }

    async #get(repository: Repository<any>, query: LivequeryRequest) {
        const { is_collection, options, keys, filters } = query

        const query_params: FindManyOptions = {
            where: {
                ...keys
            },
            take: options._limit + 1,
            order: {
                ...options._order_by ? { [options._order_by as string]: options._sort?.toUpperCase() == 'ASC' ? 'ASC' : 'DESC' } : {},
                ...options._order_by == 'created_at' ? {} : { created_at: 'DESC' }
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
            const prev_cursor = Cursor.decode<any>(options._cursor || Date.now())
            for (const [key, value] of Object.entries(prev_cursor)) {
                query_params.where[key] = (key == 'created_at' || options._sort?.toUpperCase() == 'DESC') ? LessThan(value) : MoreThan(value)
            }
        }



        // Add like expression
        const like_expressions = filters.filter(f => f[1] == 'like')
        if (like_expressions.length > 0) {
            query_params.where = like_expressions.map(([key, _, value]) => ({
                [key]: ILike(`%${value}%`),
                ...query_params.where as any
            }))
        }

        // Document query
        if (!is_collection) {
            const data = await repository.findOne(query_params) ?? null
            return { item: data }
        }

        // Collection query

        const data = await repository.find(query_params)
        const has_more = data.length > options._limit
        const items = data.slice(0, options._limit)
        const last_item = items[options._limit - 1]
        const next_cursor = !has_more ? null : Cursor.encode({
            created_at: last_item.created_at,
            [options._order_by]: last_item[options._order_by]
        })

        return {
            items,
            paging: { has_more, next_cursor }
        }
    }

    async #post(repository: Repository<any>, query: LivequeryRequest) {
        const data = { id: v4(), ...query.body }
        return await repository.save(data)
    }

    async #put(repository: Repository<any>, query: LivequeryRequest) {
        return await repository.update(query.keys, query.body)
    }

    async #patch(repository: Repository<any>, query: LivequeryRequest) {
        return await repository.update(query.keys, query.body)
    }

    async #del(repository: Repository<any>, query: LivequeryRequest) {
        return await repository.delete(query.keys)
    }

    async active_postgres_sync() {
        await this.#init_progress

        const subscribers = new Map<DataSource, {
            subscriber: Subscriber,
            tables: Map<string, {
                function_name: string,
                repository: Repository<any>
            }>
        }>()

        // Init subscribers
        for (const repository of this.#realtime_repositories) {

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
            return fromEvent<DataChangePayload>(subscriber.notifications, 'realtime_sync')
                .pipe(
                    filter(payload => payload.type == 'removed' || !!payload.data),
                    mergeMap(({ refs, type, id, data: updated_values = {}, new_doc }) => {
                        const data = JsonUtil.deepJsonParse({ id, ...updated_values || {} })
                        if (type == 'added' || type == 'removed') return refs.filter(r => r).map(({ ref }) => ({ ref, data, type }))
                        if (type == 'modified') return refs.filter(r => r).reduce((p, { old_ref, ref }) => {
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

