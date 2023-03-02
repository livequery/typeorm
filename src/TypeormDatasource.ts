import { Between, DataSource, FindManyOptions, ILike, LessThan, LessThanOrEqual, MoreThan, MoreThanOrEqual, Not, Repository, And, FindOperator, DataSourceOptions } from "typeorm";
import { LivequeryRequest } from '@livequery/types'
import { Subject } from "rxjs";
import { Cursor } from './helpers/Cursor'
import { randomUUID } from 'crypto'
import { RouteOptions } from "./RouteOptions";
import { DEFAULT_SORT_FIELD } from "./const";
import { MongoDBMapper } from "./helpers/MongoDBMapper";

const ExpressionMapper = {
    eq: { sql: v => v, mongodb: v => ({ $eq: v }) },
    ne: { sql: v => Not(v), mongodb: v => ({ $ne: v }) },
    lt: { sql: v => LessThan(v), mongodb: v => ({ $lt: v }) },
    lte: { sql: v => LessThanOrEqual(v), mongodb: v => ({ $lte: v }) },
    gt: { sql: v => MoreThan(v), mongodb: v => ({ $gt: v }) },
    gte: { sql: v => MoreThanOrEqual(v), mongodb: v => ({ $gte: v }) },
    between: { sql: ([a, b]) => Between(a, b), mongodb: ([a, b]) => ({ $gte: a, $lt: b }) }
}

export class TypeormDatasource {

    #refs_map = new Map<string, { repository: Repository<any>, db_type: DataSourceOptions['type'] }>()
    #repositories_map = new Map<Repository<any>, Set<string>>()
    #entities_map = new Map<any, Repository<any>>()
    #realtime_repositories: Repository<any>[] = []
    public readonly changes = new Subject()

    #init_progress: Promise<void>

    constructor(
        private connections: Array<DataSource & { name: string }>,
        private config: Array<RouteOptions & { refs: string[] }>
    ) {
        this.#init_progress = this.#init()
    }

    async #init() {
        for (const { connection = 'default', entity, refs, realtime = false } of this.config) {
            for (const ref of refs) {
                const datasource = this.connections.find(c => c.name == connection)
                const db_type = datasource.options.type
                if (!datasource) throw new Error(`Can not find [${connection}] datasource`)

                const repository = this.#entities_map.get(entity) ?? await datasource.getRepository(entity)
                this.#entities_map.set(entity, repository)

                const schema_ref = ref.replaceAll(':', '')
                this.#repositories_map.set(repository, new Set([
                    ... (this.#repositories_map.get(repository) || []),
                    schema_ref
                ]))
                this.#refs_map.set(schema_ref, { repository, db_type })
                realtime && this.#realtime_repositories.push(repository)
            }
        }
    }


    async query(query: LivequeryRequest) {
        await this.#init_progress

        const config = this.#refs_map.get((query as any).schema_ref)
        if (!config) throw { status: 500, code: 'REF_NOT_FOUND', message: 'Missing ref config in livequery system' }

        const { db_type, repository } = config
        if (query.method == 'get') return this.#get(repository, query, db_type)
        if (query.method == 'post') return this.#post(repository, query, db_type)
        if (query.method == 'put') return this.#put(repository, query, db_type)
        if (query.method == 'patch') return this.#patch(repository, query, db_type)
        if (query.method == 'delete') return this.#del(repository, query, db_type)
    }

    async #get(repository: Repository<any>, { is_collection, options, keys, filters }: LivequeryRequest, db_type: DataSourceOptions['type']) {


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
                .entries(db_type == 'mongodb' ? MongoDBMapper.toMongoDBObject(keys) : keys)
                .map(([key, value]) => ([key, 'eq', value])),

        ].reduce((p, [key, ex, value]) => {
            if (!p.has(key)) p.set(key, [])
            p.get(key).push(ExpressionMapper[ex as keyof typeof ExpressionMapper][db_type == 'mongodb' ? 'mongodb' : 'sql'](value))
            return p
        }, new Map<string, object[]>())

        let addional_conditions = {}

        // Search 
        if (options._search) {
            db_type == 'mongodb' && (addional_conditions['$text'] = { $search: options._search })
            if (db_type != 'mongodb') throw { status: 500, code: `${db_type.toUpperCase()}_SEARCH_NOT_SUPPORT` }
        }

        const where = [...conditions.entries()].reduce((p, [key, conditions]) => {
            p[key] = db_type == 'mongodb' ? conditions.reduce((p, e) => ({ ...p, ...e }), {}) : And(...conditions as FindOperator<any>[])
            return p
        }, addional_conditions)



        const sort = options._sort?.toUpperCase() == 'ASC' ? 'ASC' : 'DESC'
        const order = options._order_by ? (options._order_by == DEFAULT_SORT_FIELD ? { [DEFAULT_SORT_FIELD]: sort } : {
            [options._order_by as string]: sort,
            [DEFAULT_SORT_FIELD]: 'DESC'
        }) : undefined

        const query_params: FindManyOptions = {
            where,
            take: options._limit + 1,
            ...order ? { order } : {},
            ...options._select ? { select: (options as any)._select as string[] } : {},
        }



        // Document query
        if (!is_collection) {
            const item = await repository.findOne(query_params) ?? null
            return { item: db_type == 'mongodb' ? MongoDBMapper.fromMongoDBObject(item) : item }
        }

        // Collection query
        const data = await repository.find(query_params)
        const has_more = data.length > options._limit
        const items = data.slice(0, options._limit).map(
            item => db_type == 'mongodb' ? MongoDBMapper.fromMongoDBObject(item) : item
        )
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

    async #post(repository: Repository<any>, query: LivequeryRequest, db_type: DataSourceOptions['type']) {

        const obj = new (repository.metadata.target as any)()
        const data = await repository.save({
            ...db_type == 'mongodb' ? MongoDBMapper.toMongoDBObject(obj) : obj,
            ...query.body
        })

        return {
            item: db_type == 'mongodb' ? MongoDBMapper.fromMongoDBObject(data) : data
        }
    }

    async #put(repository: Repository<any>, query: LivequeryRequest, db_type: DataSourceOptions['type']) {
        return await repository.update(
            db_type == 'mongodb' ? MongoDBMapper.toMongoDBObject(query.keys) : query.keys,
            query.body
        )
    }

    async #patch(repository: Repository<any>, query: LivequeryRequest, db_type: DataSourceOptions['type']) {
        return await repository.update(
            db_type == 'mongodb' ? MongoDBMapper.toMongoDBObject(query.keys) : query.keys,
            query.body
        )

    }

    async #del(repository: Repository<any>, query: LivequeryRequest, db_type: DataSourceOptions['type']) {
        return await repository.delete(
            db_type == 'mongodb' ? MongoDBMapper.toMongoDBObject(query.keys) : query.keys
        )
    }

    // async active_postgres_sync() {
    //     await this.#init_progress

    //     const subscribers = new Map<DataSource, {
    //         subscriber: Subscriber,
    //         tables: Map<string, {
    //             function_name: string,
    //             repository: Repository<any>
    //         }>
    //     }>()

    //     // Init subscribers
    //     for (const repository of this.#realtime_repositories) {

    //         const connection = repository.metadata.connection

    //         if (!subscribers.has(connection)) {
    //             const { database, host, port, username, password } = connection.options as any
    //             const subscriber = createPostgresSubscriber({ host, port, user: username, password, database })
    //             await subscriber.connect()
    //             subscribers.set(connection, {
    //                 subscriber,
    //                 tables: new Map()
    //             })
    //         }

    //         const { tables } = subscribers.get(connection)

    //         const table_name = repository.metadata.tableName
    //         const function_name = `listen_update_for_${table_name.replaceAll('-', '_')}`
    //         if (!tables.has(table_name)) {
    //             tables.set(table_name, { function_name, repository })
    //         }
    //     }

    //     // Active listeners
    //     for (const [_, { subscriber, tables }] of subscribers) {
    //         for (const [table_name, { function_name, repository }] of tables) {
    //             const refs = [...this.#repositories_map.get(repository).values()]
    //             const CreateUpdateListenerCMD = CreateUpdateListenerSqlBuilder(function_name, refs)
    //             await repository.query(CreateUpdateListenerCMD)
    //             const CreateTableTriggerCMD = CreateTableTriggerSqlBuilder(table_name, function_name)
    //             await repository.query(CreateTableTriggerCMD)
    //         }
    //         await subscriber.listenTo('realtime_sync')
    //         return fromEvent<DataChangePayload>(subscriber.notifications, 'realtime_sync')
    //             .pipe(
    //                 filter(payload => payload.type == 'removed' || !!payload.data),
    //                 mergeMap(({ refs, type, id, data: updated_values = {}, new_doc }) => {
    //                     const data = JsonUtil.deepJsonParse({ id, ...updated_values || {} })
    //                     if (type == 'added' || type == 'removed') return refs.filter(r => r).map(({ ref }) => ({ ref, data, type }))
    //                     if (type == 'modified') return refs.filter(r => r).reduce((p, { old_ref, ref }) => {
    //                         if (old_ref == ref) {
    //                             p.push({ data, ref, type })
    //                         } else {
    //                             p.push({ data: { id: new_doc.id }, ref: old_ref, type: 'removed' })
    //                             p.push({ data: new_doc, ref, type: 'added' })
    //                         }
    //                         return p
    //                     }, [] as UpdatedData[])
    //                     return [] as UpdatedData[]
    //                 })
    //             )
    //             .subscribe(this.changes)
    //     }

    // }
}

