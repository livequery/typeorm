import { FindConditions, FindManyOptions, FindOneOptions, In, LessThan, LessThanOrEqual, Like, MoreThan, MoreThanOrEqual, Not, Raw, Repository } from "typeorm"
import { Livequery } from "@livequery/core"
import { Cursor } from "./Cursor"


export async function TypeormQueryMapper<T extends { created_at: number }>(
    table: Repository<T>,
    query: Livequery,
    searchable_fields: string[] = []
) {

    const query_params: FindManyOptions = {
        where: {
            created_at: query.sort == 'asc' ? MoreThan(0) : LessThan(Date.now())
        },
        take: query.limit + 1,
        order: {
            created_at: 'DESC',
            [query.order_by]: query.sort.toUpperCase() as 1 | "DESC" | "ASC" | -1
        },
        select: query.select as Array<keyof T>,
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

    for (const [_, { expression, key, value }] of Object.entries(query.filters)) {
        mapper[expression] && mapper[expression](key, value)
    }


    if (query.cursor) {
        const value = Cursor.decode(query.cursor)
        query_params.where[query.order_by] = query.sort == 'asc' ? MoreThan(value) : LessThan(value)
    }

    for (const key in query.params) query_params.where[key] = query.params[key]

    if (query.search && searchable_fields.length > 0) {
        const params = searchable_fields.reduce((p, c, i) => (p[`v${i}`] = `%${query.search}%`, p), {})
        query_params.where['id'] = Raw(alias => {
            const prefix = alias?.split('.')[0] || ''
            const q = searchable_fields.map((f, i) => ` "${prefix}"."${f}" ILIKE :v${i}`).join(' OR ')
            return `(${q})`
        }, params)
    }

    const data = await table.find(query_params)

    const has_more = data.length > query.limit
    const items = data.slice(0, query.limit)
    const cursor = has_more ? Cursor.encode(items[query.limit - 1][query.order_by]) : null

    return {
        data: {
            items,
            count: items.length,
            has_more,
            cursor
        }
    }
}