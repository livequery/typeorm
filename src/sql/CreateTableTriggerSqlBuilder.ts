export const CreateTableTriggerSqlBuilder = (table:string, function_name:string) => `
    DROP TRIGGER IF EXISTS realtime_sync_trigger on "${table}";
    CREATE TRIGGER realtime_sync_trigger
    AFTER
    INSERT
        OR
    UPDATE
        OR DELETE ON "${table}" FOR EACH ROW EXECUTE PROCEDURE ${function_name}();
`