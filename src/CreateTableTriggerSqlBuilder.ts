export const CreateTableTriggerSqlBuilder = (table:string) => `
    DROP TRIGGER IF EXISTS realtime_update_trigger on "${table}";
    CREATE TRIGGER realtime_update_trigger
    AFTER
    INSERT
        OR
    UPDATE
        OR DELETE ON "${table}" FOR EACH ROW EXECUTE PROCEDURE realtime_update_for_${table}();
`