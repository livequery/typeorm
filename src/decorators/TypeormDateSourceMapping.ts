import { applyDecorators, Get, UseInterceptors } from "@nestjs/common";
import { LivequeryInterceptor } from "@livequery/core";
import { TypeormMappingInterceptor } from "../interceptors/typeorm.mapping.interceptor";
import { TypeormDatasource } from "./TypeormDatasource";

export const TypeormDateSourceMapping = (entity: any, search_fields?: string[]) => applyDecorators(
    UseInterceptors(LivequeryInterceptor),
    TypeormDatasource({ entity, search_fields }),
    UseInterceptors(TypeormMappingInterceptor)
) 