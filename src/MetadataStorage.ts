import { EntityTarget } from "typeorm"




export type TypeormDatasourceOptions<T = any> = {
    realtime?: boolean
    entity: EntityTarget<T>,
    connection?: string
}


export type TypeormDatasourceMetadata = TypeormDatasourceOptions & {
    target: any,
    method: string
}


export const ControllerList: TypeormDatasourceMetadata[] = []