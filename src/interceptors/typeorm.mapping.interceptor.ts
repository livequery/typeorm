import { Injectable, NestInterceptor, ExecutionContext, CallHandler } from '@nestjs/common';
import { InjectEntityManager } from '@nestjs/typeorm';
import { Observable } from 'rxjs';
import { EntityManager } from 'typeorm';
import { LivequeryRequest, QueryFilterParser } from '@livequery/core';
import { listTypeormDatasources } from '../decorators/TypeormDatasource';
import { TypeormQueryMapper } from '../TypeormQueryMapper';
import { map } from 'rxjs/operators'
import { TypeormRealtimeUpdateService } from '../TypeormRealtimeUpdateListener';

@Injectable()
export class TypeormMappingInterceptor implements NestInterceptor {

    constructor(
        @InjectEntityManager() private EntityManager: EntityManager,
        private TypeormRealtimeUpdateService: TypeormRealtimeUpdateService
    ) { }

    async intercept(context: ExecutionContext, next: CallHandler): Promise<Observable<any>> {
        // Get entity
        const { constructorRef } = context as any
        const { name: method } = context.getHandler()
        const controller_datasources = listTypeormDatasources(constructorRef.prototype)
        const { entity, search_fields } = controller_datasources?.get(method)[0]
        const repository = await this.EntityManager.getRepository(entity)

        // Mapping with DB
        const request = context.switchToHttp().getRequest<LivequeryRequest>()
        if (request.method == 'GET') {
            if (request.livequery.isCollection) {
                const collection = await TypeormQueryMapper(repository, request.livequery, search_fields)
                return next.handle().pipe(map(() => collection))
            }
            const document = await repository.findOne(request.livequery.params)
            return next.handle().pipe(map(() => document))
        }

        return next.handle()



    }
}