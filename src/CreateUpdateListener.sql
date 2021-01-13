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