import { DataSource, FindManyOptions, Repository, And, FindOperator, DataSourceOptions } from "typeorm";
import { Cursor } from './helpers/Cursor.js'
import { RouteOptions } from "./RouteOptions.js";
import { DEFAULT_SORT_FIELD } from "./const.js";
import { ExpressionMapper } from "./helpers/ExpressionMapper.js";
import { LivequeryRequest, LivequeryBaseEntity, WebsocketSyncPayload, DatabaseEvent } from '@livequery/types'
import { Observable, map, mergeAll, pipe } from "rxjs";
import { ObjectId } from "bson";



type RefMetadata = { repository: Repository<any>, db_type: DataSourceOptions['type'], query_mapper?: boolean }

export class TypeormDatasource {

    #refs_map = new Map<string, RefMetadata>()
    #repositories_map = new Map<Repository<any>, Set<string>>()
    #entities_map = new Map<any, Repository<any>>()
    #realtime_repositories = new Map<string, Set<string>>()

    #init_progress: Promise<void>

    constructor(
        private connections: Array<DataSource & { name: string }>,
        private config: Array<RouteOptions & { refs: string[] }>
    ) {
        this.#init_progress = this.#init()
    }

    async #init() {
        for (const { connection_name = 'default', entity, refs, realtime = false, query_mapper } of this.config) {
            for (const ref of refs) {
                const datasource = this.connections.find(c => c.name == connection_name)
                const db_type = datasource.options.type
                if (!datasource) throw new Error(`Can not find [${connection_name}] datasource`)

                const repository = this.#entities_map.get(entity) ?? await datasource.getRepository(entity)
                this.#entities_map.set(entity, repository)

                const schema_ref = ref.replaceAll(':', '')
                this.#repositories_map.set(repository, new Set([
                    ... (this.#repositories_map.get(repository) || []),
                    schema_ref
                ]))
                this.#refs_map.set(schema_ref, { repository, db_type, query_mapper })
                if (realtime) {
                    const table_name = repository.metadata.tableName
                    const $ = schema_ref.split('/')
                    const is_collection_ref = $.length % 2 == 1
                    this.#realtime_repositories.set(table_name, new Set([
                        ... this.#realtime_repositories.get(table_name) || [],
                        is_collection_ref ? schema_ref : $.slice(0, -1).join('/')
                    ]))
                }
            }
        }
    }

    pipe2websocket<T extends LivequeryBaseEntity = LivequeryBaseEntity>() {
        return pipe<Observable<DatabaseEvent<T>>, Observable<WebsocketSyncPayload<T>[]>, Observable<WebsocketSyncPayload<T>>>(
            map((event: DatabaseEvent<T>) => {
                const refs = this.#realtime_repositories.get(event.table)
                if (!refs) return []
                const data = {
                    ...event.old_data || {},
                    ...event.new_data || {}
                }
                return [...refs].map(ref => {
                    const old_ref = event.old_data ? ref.split('/').map((k, i) => i % 2 == 0 ? k : event.old_data[k]).join('/') : null
                    const new_ref = event.new_data ? ref.split('/').map((k, i) => i % 2 == 0 ? k : data[k]).join('/') : null
                    return {
                        type: event.type,
                        table: event.table,
                        old_ref,
                        new_ref,
                        old_data: event.old_data as T,
                        new_data: event.new_data as T
                    } as WebsocketSyncPayload<T>
                })
            }),
            mergeAll()
        )
    }


    async query(query: LivequeryRequest) {
        await this.#init_progress

        const config = this.#refs_map.get((query as any).schema_ref)
        if (!config) throw { status: 500, code: 'REF_NOT_FOUND', message: 'Missing ref config in livequery system' }

        if (config.query_mapper) return TypeormDatasource.generate_query_filters(query, config.db_type)
        if (query.method == 'get') return this.#get(query, config)
        if (query.method == 'post') return this.#post(query, config)
        if (query.method == 'put') return this.#put(query, config)
        if (query.method == 'patch') return this.#patch(query, config)
        if (query.method == 'delete') return this.#del(query, config)
    }

    static generate_query_filters({ options, keys, filters }: LivequeryRequest, db_type: DataSourceOptions['type']) {
        const conditions = [

            // Client filters
            ...filters,

            // Cursor
            ...Object
                .entries(Cursor.decode<any>(options._cursor))
                .map(([key, value]) => [
                    key,
                    `${options._sort?.toUpperCase() == 'ASC' ? 'gt' : 'lt'}${(key != DEFAULT_SORT_FIELD ? 'e' : '')}`,
                    value
                ]),

            // Keys
            ...Object
                .entries(keys)
                .map(([key, value]) => ([key, 'eq', value])),

        ].reduce((p, [key, ex, value]) => {
            if (!p.has(key)) p.set(key, [])
            const resolver = ExpressionMapper[ex as keyof typeof ExpressionMapper]
            if (!resolver) throw { status: 500, code: `QUERY_${ex.toUpperCase()}_NOT_SUPPORT` }
            p.get(key).push(resolver[db_type == 'mongodb' ? 'mongodb' : 'sql'](value))
            return p
        }, new Map<string, object[]>())


        let addional_conditions = {}



        return [...conditions.entries()].reduce((p, [key, conditions]) => {
            if (key == '_search') {
                if (db_type == 'mongodb') {
                    p['$text'] = { $search: `${conditions[0]['$eq']}` }
                } else {
                    throw { status: 500, code: `${db_type.toUpperCase()}_SEARCH_NOT_SUPPORT` };
                }
            } else {
                p[key] = db_type == 'mongodb' ? conditions.reduce((p, e) => ({ ...p, ...e }), {}) : And(...conditions as FindOperator<any>[])
            }

            return p
        }, addional_conditions)

    }

    async #get(req: LivequeryRequest, { db_type, repository }: RefMetadata) {


        const query = TypeormDatasource.generate_query_filters(req, db_type)
        const where=  {
            ...query,
            ...  query['id'] && db_type == 'mongodb' ? {
                id: undefined,
                _id: new ObjectId(query['id'].$eq)
            }:{}
        }

        const { options, is_collection } = req

        const sort = options._sort?.toUpperCase() == 'ASC' ? 'ASC' : 'DESC'

        const order = (!options._order_by || (options._order_by == DEFAULT_SORT_FIELD)) ? { [DEFAULT_SORT_FIELD]: sort } : {
            [options._order_by]: sort,
            [DEFAULT_SORT_FIELD]: 'DESC'
        }

        const query_params: FindManyOptions = {
            where,
            take: options._limit + 1,
            ...order ? { order } : {},
            ...options._select ? { select: (options as any)._select as string[] } : {},
        }


        // Document query
        if (!is_collection) {
            const item = await repository.findOne(query_params) ?? null
            return { item }
        }

        // Collection query
        const data = await repository.find(query_params)
        const has_more = data.length > options._limit
        const items = data.slice(0, options._limit)
        const last_item = items[options._limit - 1]
        const next_cursor = !has_more ? null : Cursor.encode({
            [DEFAULT_SORT_FIELD]: last_item[DEFAULT_SORT_FIELD],
            [options._order_by]: last_item[options._order_by]
        })

        return {
            items,
            paging: { has_more, next_cursor }
        }
    }

    async #post(req: LivequeryRequest, { repository }: RefMetadata) {
        const merged = {
            ...new (repository.metadata.target as any)(),
            ...req.keys,
            ...req.body
        }
        const { _id, id } = merged
        const data = await repository.save(merged)
        return {
            item: data
        }
    }

    async #put(req: LivequeryRequest, { repository }: RefMetadata) {
        return await repository.update(
            req.keys,
            req.body
        )
    }

    async #patch(req: LivequeryRequest, {  repository }: RefMetadata) {
        return await repository.update(
            req.keys,
            req.body
        )

    }

    async #del(req: LivequeryRequest, { repository }: RefMetadata) {
        return await repository.delete(
            req.keys
        )
    }
}

