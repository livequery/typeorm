import { DynamicModule, Module, OnModuleInit } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { SocketGateway } from '@livequery/core';
import { TypeormMappingInterceptor } from './interceptors/typeorm.mapping.interceptor';
import { TypeormRealtimeUpdateService } from './TypeormRealtimeUpdateListener';


@Module({
    imports: [TypeOrmModule],
    providers: [
        TypeormMappingInterceptor,
        TypeormRealtimeUpdateService.AsyncProvider 
    ],
    exports: [
        TypeormRealtimeUpdateService
    ]
})
export class TypeormBindingLivequeryModule { } 