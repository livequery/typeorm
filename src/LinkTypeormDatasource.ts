
import { ControllerList, TypeormDatasourceOptions } from "./MetadataStorage";



export const LinkTypeormDatasource = (options: TypeormDatasourceOptions) => (target, method: string) => {
    ControllerList.push({ target, method, ...options })
}