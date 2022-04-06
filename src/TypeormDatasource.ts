import { Between, Connection, FindManyOptions, getConnection, getRepository, ILike, LessThan, LessThanOrEqual, MoreThan, MoreThanOrEqual, Not, Repository } from "typeorm";
import { LivequeryRequest, UpdatedData } from '@livequery/types'
import { filter, fromEvent, mergeMap, of, Subject } from "rxjs";
import { DataChangePayload } from "./DataChangePayload";
import { JsonUtil } from "./helpers/JsonUtil";
import { CreateTableTriggerSqlBuilder } from "./sql/CreateTableTriggerSqlBuilder";
import { CreateUpdateListenerSqlBuilder } from "./sql/CreateUpdateListenerSqlBuilder";
import { Cursor } from './helpers/Cursor'
import { v4 } from 'uuid'
import createPostgresSubscriber, { Subscriber } from "pg-listen"
import { ControllerList } from "./MetadataStorage";
import { PathMapper } from "./helpers/PathMapper";

const LIVEQUERY_MAGIC_KEY = `${process.env.LIVEQUERY_MAGIC_KEY || 'livequery'}/`


export class TypeormDatasource   {

    #refs_map = new Map<string, Repository<any>>()
    #repositories_map = new Map<Repository<any>, Set<string>>()
    #entities_map = new Map<any, Repository<any>>()
    #id = v4()
    public readonly changes = new Subject()

      constructor() {
        console.log(`Constrcutor [${this.#id}]`)
     }
 

    async init() {

        console.log(`ID: [${this.#id }]`)



        const realtime_repositories: Repository<any>[] = []

        for (const connection of new Set(ControllerList.map(c => c.connection || 'default'))) {
            try { await getConnection(connection) } catch (e) {
                throw new Error(`Database connection [${connection}] not found, please check TypeormDatasource dependencies (you can add one repository as dependency)`)
            }
        }

        for (const { connection = 'default', entity, method, target, realtime = false } of ControllerList) {
            for (const fullpath of PathMapper(
                Reflect.getMetadata('path', target.constructor),
                Reflect.getMetadata('path', target.constructor, method)
            )) {

                const repository = this.#entities_map.get(entity) ?? await getRepository(entity, connection)
                this.#entities_map.set(entity, repository)
                if (!fullpath.includes(LIVEQUERY_MAGIC_KEY)) throw new Error(`Path "${fullpath}" doesn't includes "${LIVEQUERY_MAGIC_KEY}"`)
                const ref = fullpath.split(LIVEQUERY_MAGIC_KEY)[1]

                const schema_ref = ref.replaceAll(':', '')


                this.#repositories_map.set(repository, new Set([
                    ... (this.#repositories_map.get(repository) || []),
                    schema_ref
                ]))

                this.#refs_map.set(schema_ref, repository)

                realtime && realtime_repositories.push(repository)
            }
        }

        await this.active_postgres_sync(realtime_repositories)

    }

   

    async query(query: LivequeryRequest) {
        console.log({query})
        const repository = this.#refs_map.get(query.schema_collection_ref)
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

    async active_postgres_sync(repositories: Repository<any>[]) {

        const subscribers = new Map<Connection, {
            subscriber: Subscriber,
            tables: Map<string, {
                function_name: string,
                repository: Repository<any>
            }>
        }>()

        // Init subscribers
        for (const repository of repositories) {

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

