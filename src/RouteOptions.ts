import { EntityTarget } from "typeorm"

export type RouteOptions<T = any> = {
    realtime?: boolean
    entity: EntityTarget<T>,
    connection_name?: string
    query_mapper?: boolean
}
