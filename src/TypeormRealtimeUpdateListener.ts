import { readFileSync } from "fs";
import createSubscriber, { Subscriber } from "pg-listen"
import { SocketGateway, RealtimeUpdateItem } from '@livequery/core'
import { Injectable, Logger, Provider } from "@nestjs/common"
import { getConnection, getConnectionOptions } from 'typeorm'
import { PostgresConnectionOptions } from "typeorm/driver/postgres/PostgresConnectionOptions";


const CreateUpdateListener = `
-- Trigger notification for messaging to PG Notify
CREATE OR REPLACE FUNCTION realtime_update() RETURNS trigger AS $trigger$
DECLARE
  updated_values jsonb;
  id text;
  path text;
BEGIN
  -- Set record row depending on operation
  CASE TG_OP
  WHEN 'INSERT' THEN
        updated_values := to_jsonb(NEW);
        id := NEW.id;
  WHEN 'UPDATE' THEN
        SELECT jsonb_object_agg(n.key, n.value)
        INTO updated_values
        FROM jsonb_each(to_jsonb(OLD)) o
        JOIN jsonb_each(to_jsonb(NEW)) n USING (key)
        WHERE n.value IS DISTINCT FROM o.value; 
        id := NEW.id;
  WHEN 'DELETE' THEN
        updated_values := to_jsonb(OLD);
        id := OLD.id;
  ELSE
     RAISE EXCEPTION 'Unknown TG_OP: "%". Should not occur!', TG_OP;
  END CASE; 

  -- Notify the channel 
  PERFORM pg_notify('realtime_update', json_build_object(
        'type', TG_OP,
        'id',   COALESCE(NEW.id, OLD.id)::text,
        'ref',  COALESCE(NEW.ref, OLD.ref)::text,
        'data', updated_values
  )::text);
  
  RETURN NULL;
END;
$trigger$ LANGUAGE plpgsql;
`
const CreateTableTrigger = `
DROP TRIGGER IF EXISTS realtime_update_trigger on "__TABLE__";
CREATE TRIGGER realtime_update_trigger
AFTER
INSERT
    OR
UPDATE
    OR DELETE ON "__TABLE__" FOR EACH ROW EXECUTE PROCEDURE realtime_update();
`

const sleep = ms => new Promise(s => setTimeout(s, ms))

@Injectable()
export class TypeormRealtimeUpdateService {

    private constructor(private socket_gateway: SocketGateway) { }

    static AsyncProvider: Provider = {
        provide: TypeormRealtimeUpdateService,
        useFactory: async (socket_gateway: SocketGateway) => {
            const s = new TypeormRealtimeUpdateService(socket_gateway)
            await s.active()
            return s
        },
        inject: [SocketGateway]
    }

    private async active() {
        while (!await getConnection().isConnected) {
            Logger.warn('Initing realtime database update ... ')
            await sleep(1000)
        }
        const connection = await getConnection()
        const list = await connection.query(`select table_name from information_schema.columns where column_name = 'ref' `) as Array<{ table_name }>
        const tables = list.map(l => l.table_name)

        Logger.warn(`Created realtime triggers on ${tables.length} tables: ${tables.join(', ')}`)

        // Create trigger
        await connection.query(CreateUpdateListener)
        for (const table_name of tables) {
            const script = CreateTableTrigger.replace(
                /__TABLE__/g,
                table_name
            )
            await connection.query(script)
        }

        // Create trigger 
        const { host, port, username, password, database } = await connection.options as PostgresConnectionOptions
        const subscriber = createSubscriber({ host, port, user: username, password, database })
        await subscriber.connect()
        subscriber.listenTo('realtime_update')
        subscriber.notifications.on('realtime_update', (payload: RealtimeUpdateItem<any>) => {
            if (!payload.data) return
            this.socket_gateway.broadcast(payload)
        })
    }
}