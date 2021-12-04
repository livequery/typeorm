import { EntityTarget } from "typeorm"

export const UseTypeormDatasourceMetadata = Symbol()

export type UseTypeormDatasourceMetadata<T = any> = {
    schema_ref: string,
    entity: EntityTarget<T>,
    connection: string
}