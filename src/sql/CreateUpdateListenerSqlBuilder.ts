const ref_builder = (from: string, ref: string) => ref.split('/').map((key, i, { length }) => i % 2 == 0 ? `'${i == 0 ? '' : '/'}${key}${i == length - 1 ? '' : '/'}'` : `${from}.${key}`).join(' || ')

export const CreateUpdateListenerSqlBuilder = (function_name: string, refs: string[]) => `

      
      CREATE OR REPLACE FUNCTION ${function_name}() RETURNS trigger AS $trigger$
      DECLARE
        updated_values jsonb;
        doc jsonb;
        id text;
        type text;
        refs json[];
      BEGIN

            refs = '{}'::json[]; 
            ${refs
            .map(ref => `refs := array_append(refs, ('{"ref":"' || ${ref_builder('NEW', ref)} || '", "old_ref":"' || ${ref_builder('OLD', ref)} || '"}')::json); `)
            .join('\n')
      } 
            
            CASE TG_OP
            WHEN 'INSERT' THEN
                  updated_values := to_jsonb(NEW);
                  id := NEW.id;
                  type := 'added'; 
            WHEN 'UPDATE' THEN
                  SELECT jsonb_object_agg(n.key, n.value)
                  INTO updated_values
                  FROM jsonb_each(to_jsonb(OLD)) o
                  JOIN jsonb_each(to_jsonb(NEW)) n USING (key)
                  WHERE n.value IS DISTINCT FROM o.value; 
                  id := NEW.id;
                  type := 'modified'; 
                  doc := to_jsonb(NEW); 
            WHEN 'DELETE' THEN 
                  id := OLD.id;
                  type := 'removed';
            ELSE
                  RAISE EXCEPTION 'Unknown TG_OP: "%". Should not occur!', TG_OP;
            END CASE; 
      
            PERFORM pg_notify(
                  'realtime_sync', 
                  json_build_object(
                        'type' ,   type,
                        'id'   ,   id, 
                        'data' ,   updated_values,
                        'refs' ,   refs,
                        'doc'  ,   doc
                  )::text
            );
        
        RETURN NULL;
      END;
      $trigger$ LANGUAGE plpgsql;
`