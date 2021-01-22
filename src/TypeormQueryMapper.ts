import { FindConditions, In, LessThan, LessThanOrEqual, Like, MoreThan, MoreThanOrEqual, Not, Raw, Repository } from "typeorm"
import { Livequery } from "@livequery/core"


export async function TypeormQueryMapper<T>(
    table: Repository<T>,
    query: Livequery,
    searchable_fields: string[] = []
) {

    let where: FindConditions<T> = {}

    const mapper = {
        eq: (key: string, value: any) => where[key] = value,
        like: (key: string, value: any) => where[key] = Like(value),
        ne: (key: string, value: any) => where[key] = Not(value),
        lt: (key: string, value: any) => where[key] = LessThan(value),
        lte: (key: string, value: any) => where[key] = LessThanOrEqual(value),
        gt: (key: string, value: any) => where[key] = MoreThan(value),
        gte: (key: string, value: any) => where[key] = MoreThanOrEqual(value),
        in: (key: string, value: any) => where[key] = In(value),
    }

    for (const [_, { expression, key, value }] of Object.entries(query.filters)) {
        mapper[expression] && mapper[expression](key, value)
    }

    if (query.cursor || where[query.order_by]) {
        const compareFunction = query.sort == 'asc' ? MoreThan : LessThan
        const compareValue = query.cursor ? JSON.parse(Buffer.from(query.cursor, 'base64').toString('utf8')) : (query.sort == 'asc' ? 0 : Date.now())
        where[query.order_by] = compareFunction(compareValue)
    }

    for (const key in query.params) where[key] = query.params[key]

    if (query.search && searchable_fields.length > 0) {
        const params = searchable_fields.reduce((p, c, i) => (p[`v${i}`] = `%${query.search}%`, p), {})
        where['id'] = Raw(alias => {
            const prefix = alias?.split('.')[0] || ''
            const q = searchable_fields.map((f, i) => ` "${prefix}"."${f}" ILIKE :v${i}`).join(' OR ')
            return `(${q})`
        }, params)
    } 

    const data = await table.find({
        where,
        take: query.limit + 1,
        order: {
            [query.order_by]: query.sort.toUpperCase()
        } as any,
        select: query.select as Array<keyof T>,

    })

    const has_more = data.length > query.limit
    const items = data.slice(0, query.limit)


    const cursor = has_more ? Buffer.from(JSON.stringify(items[query.limit - 1][query.order_by])).toString('base64') : null


    return {
        data: {
            items,
            count: items.length,
            has_more,
            cursor
        }
    }
}