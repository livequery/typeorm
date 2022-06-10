import { EntityTarget } from "typeorm"

export type RouteOptions<T = any> = {
    realtime?: boolean
    entity: EntityTarget<T>,
    connection?: string
}
