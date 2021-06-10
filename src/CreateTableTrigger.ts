export const CreateTableTrigger = `
DROP TRIGGER IF EXISTS realtime_update_trigger on "__TABLE__";
CREATE TRIGGER realtime_update_trigger
AFTER
INSERT
    OR
UPDATE
    OR DELETE ON "__TABLE__" FOR EACH ROW EXECUTE PROCEDURE realtime_update();
`