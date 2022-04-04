
import { applyDecorators, UseInterceptors } from "@nestjs/common";
import { ControllerList, UseTypeormDatasourceOptions } from "./MetadataStorage";
import { TypeormDatasource } from "./TypeormDatasource";



export const UseTypeormDatasource = (options: UseTypeormDatasourceOptions) => applyDecorators(
    (target, method: string) => ControllerList.push({ target, method, ...options }), 
    UseInterceptors(TypeormDatasource)

)