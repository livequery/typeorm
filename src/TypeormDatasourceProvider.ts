import { createDatasourceMapper } from "@livequery/nestjs";
import { RouteOptions } from "./RouteOptions";
import { TypeormDatasource } from "./TypeormDatasource";
import { DataSource } from 'typeorm'
import { getConnectionToken } from '@nestjs/typeorm'
import { getEntityName } from "./helpers/getEntityName";

export const [UseTypeormDatasource, getMetadatas] = createDatasourceMapper<RouteOptions>(TypeormDatasource)


export const TypeormDatasourceProviderWithMultipleConnections = (connection_names: string[] = []) => {

    return {
        provide: TypeormDatasource,
        inject: [undefined, ...connection_names].map(connection_name => getConnectionToken(connection_name)),
        useFactory: (...connections: DataSource[]) => {

            const options_with_name = getMetadatas().map(option => ({
                ...option,
                collection_name: getEntityName(option.entity)
            }))

            return new TypeormDatasource(
                connections,
                options_with_name
            )
        }
    }
}


export const TypeormDatasourceProvider = TypeormDatasourceProviderWithMultipleConnections() 