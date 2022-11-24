import { createDatasourceMapper } from "@livequery/nestjs";
import { RouteOptions } from "./RouteOptions";
import { TypeormDatasource } from "./TypeormDatasource";
import { DataSource } from 'typeorm'
import { getConnectionToken } from '@nestjs/typeorm'
import { getEntityName } from "./helpers/getEntityName";

export const [_, UseTypeormDatasource] = createDatasourceMapper<RouteOptions>(TypeormDatasource)

export const TypeormDatasourceProvider = (connection_names: string[] = []) => _(options => {

    const options_with_name = options.map(option => ({
        ...option,
        collection_name: getEntityName(option.entity)
    }))

    return {
        provide: TypeormDatasource,
        inject: ['default', ...connection_names].map(connection_name => getConnectionToken(connection_name)),
        useFactory: (connections: DataSource[]) => new TypeormDatasource(
            connections,
            options_with_name
        )
    }
})