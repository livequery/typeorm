import { EntityTarget } from "typeorm"




export type UseTypeormDatasourceOptions<T = any> = {
    realtime?: boolean
    entity: EntityTarget<T>,
    connection?: string
}


export type TypeormDatasourceMetadata = UseTypeormDatasourceOptions & {
    target: any,
    method: string
}


export const ControllerList: TypeormDatasourceMetadata[] = []