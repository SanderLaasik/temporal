CREATE SCHEMA TEMPORAL;

CREATE TABLE TEMPORAL.TEMPORAL_METADATA (
	Schema_Name VARCHAR(63) NOT NULL,
	Table_Name VARCHAR(63) NOT NULL,
	Property_Column_List TEXT[] NOT NULL,
	Is_Property_PK BOOLEAN NOT NULL,
	Since_Column_Name VARCHAR(63) NOT NULL,
	Hist_Table_Name VARCHAR(63) NOT NULL,
	Combining_View_Name VARCHAR(63) NOT NULL,
	CONSTRAINT PK_Metadata PRIMARY KEY (Schema_Name, Table_Name, Property_Column_List)
);

CREATE OR REPLACE FUNCTION TEMPORAL.FIRST_DATE () RETURNS DATE AS $$
        BEGIN
                RETURN '4713-01-01 BC';
        END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION TEMPORAL.LAST_DATE () RETURNS DATE AS $$
        BEGIN
                RETURN '294276-01-01 AD';
        END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION TEMPORAL.NEXT_DATE (Param_Date DATE) RETURNS DATE AS $$
        BEGIN
                RETURN Param_Date + 1;
        END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION TEMPORAL.PRIOR_DATE (Param_Date DATE) RETURNS DATE AS $$
        BEGIN
                RETURN Param_Date - 1;
        END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION TEMPORAL.BEGIN (Param_Interval DATERANGE) RETURNS DATE AS $$
        BEGIN
                RETURN LOWER(Param_Interval);
        END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION TEMPORAL.CONTAINED_IN (Param_Date DATE, Param_Interval DATERANGE) RETURNS BOOLEAN AS $$
        BEGIN
                RETURN Param_Date>=TEMPORAL.BEGIN(Param_Interval) AND Param_Date<=TEMPORAL.END(Param_Interval);
        END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION TEMPORAL.CONTAINS (Param_Interval DATERANGE, Param_Date DATE) RETURNS BOOLEAN AS $$
        BEGIN
                RETURN TEMPORAL.CONTAINED_IN(Param_Date, Param_Interval);
        END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION TEMPORAL.END (Param_Interval DATERANGE) RETURNS DATE AS $$
        BEGIN
                RETURN UPPER(Param_Interval)-1;
        END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION TEMPORAL.POST (Param_Interval DATERANGE) RETURNS DATE AS $$
        BEGIN
                RETURN TEMPORAL.END(Param_Interval)+1;
        END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION TEMPORAL.PRE (Param_Interval DATERANGE) RETURNS DATE AS $$
        BEGIN
                RETURN TEMPORAL.BEGIN(Param_Interval)-1;
        END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION TEMPORAL.AFTER (Param_Interval1 DATERANGE, Param_Interval2 DATERANGE) RETURNS BOOLEAN AS $$
        BEGIN
                RETURN TEMPORAL.BEFORE(Param_Interval2, Param_Interval1);
        END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION TEMPORAL.BEFORE (Param_Interval1 DATERANGE, Param_Interval2 DATERANGE) RETURNS BOOLEAN AS $$
        BEGIN
                RETURN TEMPORAL.END(Param_Interval1) < TEMPORAL.BEGIN(Param_Interval2);
        END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION TEMPORAL.BEGINS (Param_Interval1 DATERANGE, Param_Interval2 DATERANGE) RETURNS BOOLEAN AS $$
        BEGIN
                RETURN 
			TEMPORAL.BEGIN(Param_Interval1)=TEMPORAL.BEGIN(Param_Interval2)
			AND TEMPORAL.END(Param_Interval1)<=TEMPORAL.END(Param_Interval2)
		;
        END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION TEMPORAL.ENDS (Param_Interval1 DATERANGE, Param_Interval2 DATERANGE) RETURNS BOOLEAN AS $$
        BEGIN
                RETURN 
			TEMPORAL.END(Param_Interval1)=TEMPORAL.END(Param_Interval2)
			AND TEMPORAL.BEGIN(Param_Interval1)>=TEMPORAL.BEGIN(Param_Interval2)
		;
        END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION TEMPORAL.EQUALS (Param_Interval1 DATERANGE, Param_Interval2 DATERANGE) RETURNS BOOLEAN AS $$
        BEGIN
                RETURN 
			TEMPORAL.BEGIN(Param_Interval1)=TEMPORAL.BEGIN(Param_Interval2) 
			AND TEMPORAL.END(Param_Interval1)=TEMPORAL.END(Param_Interval2);
        END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION TEMPORAL.INCLUDED_IN (Param_Interval1 DATERANGE, Param_Interval2 DATERANGE) RETURNS BOOLEAN AS $$
        BEGIN
                RETURN TEMPORAL.INCLUDES(Param_Interval2, Param_Interval1);
        END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION TEMPORAL.INCLUDES (Param_Interval1 DATERANGE, Param_Interval2 DATERANGE) RETURNS BOOLEAN AS $$
        BEGIN
                RETURN 
			TEMPORAL.BEGIN(Param_Interval1)<=TEMPORAL.BEGIN(Param_Interval2)
			AND TEMPORAL.END(Param_Interval1)>=TEMPORAL.END(Param_Interval2);
        END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION TEMPORAL.MEETS (Param_Interval1 DATERANGE, Param_Interval2 DATERANGE) RETURNS BOOLEAN AS $$
        BEGIN
                RETURN 
			TEMPORAL.BEGIN(Param_Interval2) = TEMPORAL.END(Param_Interval1)+1
			OR TEMPORAL.BEGIN(Param_Interval1) = TEMPORAL.END(Param_Interval2)+1
		;
        END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION TEMPORAL.MERGES(Param_Interval1 DATERANGE, Param_Interval2 DATERANGE) RETURNS BOOLEAN AS $$
        BEGIN
                RETURN 
			TEMPORAL.OVERLAPS(Param_Interval1, Param_Interval2)
			OR TEMPORAL.MEETS(Param_Interval1, Param_Interval2)
		;
        END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION TEMPORAL.OVERLAPS (Param_Interval1 DATERANGE, Param_Interval2 DATERANGE) RETURNS BOOLEAN AS $$
        BEGIN
                RETURN 
			TEMPORAL.BEGIN(Param_Interval1) <= TEMPORAL.END(Param_Interval2)
			AND TEMPORAL.BEGIN(Param_Interval2) <= TEMPORAL.END(Param_Interval1)
		;
        END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION TEMPORAL.COUNT(Param_Interval DATERANGE) RETURNS INTEGER AS $$
        BEGIN
                RETURN (TEMPORAL.END(Param_Interval)-TEMPORAL.BEGIN(Param_Interval))+1;
        END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION TEMPORAL.INTERSECT(Param_Interval1 DATERANGE, Param_Interval2 DATERANGE) RETURNS DATERANGE AS $$
	DECLARE
		v_interval_start DATE;   
		v_interval_end DATE;
	BEGIN
		IF 
			TEMPORAL.OVERLAPS(Param_Interval1, Param_Interval2)
		THEN 	
			v_interval_start := TEMPORAL.MAX(TEMPORAL.BEGIN(Param_Interval1),TEMPORAL.BEGIN(Param_Interval2));
			v_interval_end := TEMPORAL.MIN(TEMPORAL.END(Param_Interval1),TEMPORAL.END(Param_Interval2));
			RETURN DATERANGE ('['||v_interval_start||','||v_interval_end||']');
		ELSE RETURN DATERANGE 'EMPTY';
		END IF;
        END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION TEMPORAL.MAX(Param_Date1 DATE, Param_Date2 DATE) RETURNS DATE AS $$
        BEGIN
                RETURN CASE WHEN Param_Date1<Param_Date2 THEN Param_Date2 ELSE Param_Date1 END;
        END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION TEMPORAL.MIN(Param_Date1 DATE, Param_Date2 DATE) RETURNS DATE AS $$
        BEGIN
                RETURN CASE WHEN Param_Date1<Param_Date2 THEN Param_Date1 ELSE Param_Date2 END;
        END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION TEMPORAL.MINUS(Param_Interval1 DATERANGE, Param_Interval2 DATERANGE) RETURNS DATERANGE AS $$
	DECLARE
		v_interval_start DATE;   
		v_interval_end DATE;
	BEGIN
		IF
			TEMPORAL.BEGIN(Param_Interval1)<TEMPORAL.BEGIN(Param_Interval2) 
			AND TEMPORAL.END(Param_Interval1)<=TEMPORAL.END(Param_Interval2)
		THEN 
			v_interval_start := TEMPORAL.BEGIN(Param_Interval1);
			v_interval_end := TEMPORAL.MIN(TEMPORAL.BEGIN(Param_Interval2)-1, TEMPORAL.END(Param_Interval1));
		ELSEIF
			TEMPORAL.BEGIN(Param_Interval1)>=TEMPORAL.BEGIN(Param_Interval2)
			AND TEMPORAL.END(Param_Interval1)>TEMPORAL.END(Param_Interval2)
		THEN 
			v_interval_start := TEMPORAL.MAX(TEMPORAL.END(Param_Interval2)+1, TEMPORAL.BEGIN(Param_Interval1));
			v_interval_end := TEMPORAL.END(Param_Interval1);
		ELSE RETURN DATERANGE 'EMPTY';
		END IF;
		RETURN DATERANGE ('['||v_interval_start||','||v_interval_end||']');
        END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION TEMPORAL.UNION(Param_Interval1 DATERANGE, Param_Interval2 DATERANGE) RETURNS DATERANGE AS $$
	DECLARE
		v_interval_start DATE;   
		v_interval_end DATE;
		v_interval_result DATERANGE;
	BEGIN
		v_interval_start := TEMPORAL.MIN(TEMPORAL.BEGIN(COALESCE(Param_Interval1, 'EMPTY')),TEMPORAL.BEGIN(COALESCE(Param_Interval2, 'EMPTY')));
		v_interval_end := TEMPORAL.MAX(TEMPORAL.END(COALESCE(Param_Interval1, 'EMPTY')),TEMPORAL.END(COALESCE(Param_Interval2, 'EMPTY')));

		IF 
			TEMPORAL.MERGES(COALESCE(Param_Interval1, 'EMPTY'), COALESCE(Param_Interval2, 'EMPTY')) 
			THEN v_interval_result := ('['||v_interval_start||','||v_interval_end||']');
			ELSE v_interval_result := 'EMPTY';
		END IF;
		RETURN v_interval_result;
        END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION TEMPORAL.COLLAPSE(Param_Intervals DATERANGE[]) RETURNS TABLE (Duration_Column DATERANGE) AS $$
	DECLARE
		v_num_tot_intervals INTEGER;
		v_running_non_coll INTEGER;
		v_running_coll INTEGER;

		v_running_collapsed DATERANGE;
		v_running_non_collapsed DATERANGE;
	BEGIN
		CREATE TABLE NON_COLLAPSED AS 
		SELECT UNNEST(Param_Intervals) AS Duration_Column, ROW_NUMBER() OVER (ORDER BY UNNEST(Param_Intervals)) AS RN;

		v_num_tot_intervals := (SELECT COUNT(*) FROM NON_COLLAPSED);
		v_running_coll := 1;
		v_running_non_coll := 1;

		CREATE TABLE COLLAPSED (Collapsed_Row_No INTEGER, Duration_Column DATERANGE, Non_Collapsed_Row_No INTEGER);
		

		WHILE v_running_coll<=v_num_tot_intervals LOOP
			INSERT INTO COLLAPSED (
				Collapsed_Row_No, 
				Duration_Column, 
				Non_Collapsed_Row_No)
			VALUES (
				v_running_coll, 
				(SELECT NON_COLLAPSED.Duration_Column FROM NON_COLLAPSED WHERE RN=v_running_coll),
				v_running_non_coll
			);
			WHILE v_running_non_coll<=v_num_tot_intervals LOOP
				v_running_collapsed := (
					SELECT COLLAPSED.Duration_Column
					FROM COLLAPSED
					WHERE COLLAPSED.Collapsed_Row_No=v_running_coll
					AND COLLAPSED.Non_Collapsed_Row_No=v_running_non_coll-1
				);

				v_running_non_collapsed := (
					SELECT NON_COLLAPSED.Duration_Column
					FROM NON_COLLAPSED
					WHERE RN=v_running_non_coll
				);

				IF (TEMPORAL.UNION(v_running_collapsed, v_running_non_collapsed)<>'Empty') THEN
					INSERT INTO COLLAPSED (
						Collapsed_Row_No, 
						Duration_Column, 
						Non_Collapsed_Row_No)
					SELECT
						v_running_coll, 
						TEMPORAL.UNION(v_running_collapsed, v_running_non_collapsed),
						v_running_non_coll
					FROM NON_COLLAPSED AS NC
					WHERE NC.RN=v_running_non_coll
					AND NOT EXISTS (
						SELECT 1
						FROM COLLAPSED X
						WHERE X.Duration_Column=NC.Duration_Column
					);

					DELETE
					FROM COLLAPSED
					WHERE COLLAPSED.Collapsed_Row_No=v_running_coll
					AND COLLAPSED.Non_Collapsed_Row_No=v_running_non_coll-1;

				END IF;
				v_running_non_coll = v_running_non_coll+1;
			END LOOP;
			v_running_coll = (SELECT MAX(COLLAPSED.Non_Collapsed_Row_No)+1 FROM COLLAPSED);
			v_running_non_coll = v_running_coll;
		END LOOP;


		RETURN QUERY
		SELECT COLLAPSED.Duration_Column FROM COLLAPSED;

		DROP TABLE COLLAPSED;
		DROP TABLE NON_COLLAPSED;
        END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION TEMPORAL.EXPAND(Param_Intervals DATERANGE[]) RETURNS TABLE (Unit_Interval DATERANGE) AS $$
	DECLARE
		v_num_tot_intervals INTEGER;
		v_running_interval_no INTEGER;
		v_running_interval DATERANGE;
		v_interval_start DATE; 
		v_interval_end DATE;
	BEGIN
		CREATE TEMPORARY TABLE NON_EXPANDED AS 
		SELECT UNNEST(Param_Intervals) AS Duration_Column, ROW_NUMBER() OVER (ORDER BY UNNEST(Param_Intervals)) AS RN;


		--IF EXISTS (
		--	SELECT 1 
		--	FROM NON_EXPANDED 
		--	WHERE TEMPORAL.END(Duration_Column)='INFINITY'
		--) THEN RAISE EXCEPTION 'Ranges with INFINITY are not allowed';
		--END IF;

		CREATE TEMPORARY TABLE EXPANDED_FORM (Unit_Interval DATERANGE);

		v_num_tot_intervals := (SELECT COUNT(*) FROM NON_EXPANDED);
		v_running_interval_no := 1;

		WHILE v_running_interval_no<=v_num_tot_intervals LOOP
			v_running_interval := (
				SELECT Duration_Column 
				FROM NON_EXPANDED 
				WHERE RN=v_running_interval_no
			);
			v_interval_start := TEMPORAL.BEGIN(v_running_interval);
			v_interval_end := TEMPORAL.END(v_running_interval);
			IF v_interval_end='INFINITY' THEN
				v_interval_end:=CURRENT_DATE;
			END IF;

			WHILE v_interval_start<=v_interval_end LOOP
	    		INSERT INTO EXPANDED_FORM (Unit_Interval) 
				VALUES (DATERANGE ('['||v_interval_start||','||v_interval_start||']'));
				v_interval_start = v_interval_start + 1;
			END LOOP;
			v_running_interval_no = v_running_interval_no+1;
		END LOOP;

		RETURN QUERY 
		SELECT DISTINCT EXPANDED_FORM.Unit_Interval 
		FROM EXPANDED_FORM ORDER BY EXPANDED_FORM.Unit_Interval;
		DROP TABLE NON_EXPANDED;
		DROP TABLE EXPANDED_FORM;
        END;
$$ LANGUAGE plpgsql;

/*
Statement1 - 1st statement
Statement2 - 2nd statement
Grouping columns - list of columns the COLLAPSE is grouped by. If Range column is the only one resultset then leave "Grouping columns" empty ('') or NULL
Range column - columnt that is used as parameter of COLLAPSE
*/

CREATE OR REPLACE FUNCTION TEMPORAL.TEMPORAL_EQUALS(
	Statement1 TEXT, 
	Statement2 TEXT, 
	Grouping_Columns TEXT,
	Range_Column VARCHAR(63)
) RETURNS BOOLEAN AS $$
	DECLARE 
		v_Rec RECORD;
		v_Select_List TEXT;
		v_Group_By TEXT;
		v_Order_By TEXT;
		v_Result BOOLEAN;
	BEGIN
		PERFORM TEMPORAL.PREPARE_TEMP(Statement1, 1); --INPUT_TEMP1
		PERFORM TEMPORAL.PREPARE_TEMP(Statement2, 2); --INPUT_TEMP2
		
		IF Grouping_Columns IS NULL OR CHAR_LENGTH(Grouping_Columns)=0 THEN 
			v_Select_List := '';
			v_Group_By := '';
			v_Order_By := '';
		ELSE
			v_Select_List := Grouping_Columns||', ';
			v_Group_By := 'GROUP BY '||Grouping_Columns;
			v_Order_By := 'ORDER BY '||Grouping_Columns;
		END IF;

		EXECUTE '
		SELECT COALESCE((
			SELECT COALESCE(ARRAY_TO_STRING(ARRAY_AGG(ROW(SUB1.*)), '',''), ''NO ROWS RETURNED'')
			FROM (
				SELECT '||v_Select_List||'TEMPORAL.EXPAND(ARRAY_AGG('||Range_Column||')) AS '||Range_Column||'
				FROM INPUT_TEMP1 
				'||v_Group_By||'
				'||v_Order_By||'
			) SUB1
		) = (
			SELECT COALESCE(ARRAY_TO_STRING(ARRAY_AGG(ROW(SUB2.*)), '',''), ''NO ROWS RETURNED'')
			FROM (
				SELECT '||v_Select_List||'TEMPORAL.EXPAND(ARRAY_AGG('||Range_Column||')) AS '||Range_Column||'
				FROM INPUT_TEMP2
				'||v_Group_By||'
				'||v_Order_By||'
			) SUB2
		), FALSE)' INTO v_Result;

		DROP TABLE INPUT_TEMP1;
		DROP TABLE INPUT_TEMP2;
		RETURN v_Result;
        END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION TEMPORAL.TEMPORAL_INTERSECT(
	Statement1 TEXT, 
	Statement2 TEXT, 
	Grouping_Columns TEXT,
	Range_Column VARCHAR(63)
) RETURNS SETOF RECORD AS $$
	DECLARE 
		v_Rec RECORD;
		v_Select_List TEXT;
		v_Group_By TEXT;
	BEGIN
		PERFORM TEMPORAL.PREPARE_TEMP(Statement1, 1); --INPUT_TEMP1
		PERFORM TEMPORAL.PREPARE_TEMP(Statement2, 2); --INPUT_TEMP2
		
		IF Grouping_Columns IS NULL OR CHAR_LENGTH(Grouping_Columns)=0 THEN 
			v_Select_List := '';
			v_Group_By := '';
		ELSE
			v_Select_List := Grouping_Columns||', ';
			v_Group_By := 'GROUP BY '||Grouping_Columns;
		END IF;

		RETURN QUERY 
		EXECUTE '
		SELECT '||v_Select_List||'TEMPORAL.COLLAPSE(ARRAY_AGG('||Range_Column||')) AS '||Range_Column||'
		FROM (
			SELECT '||v_Select_List||'TEMPORAL.EXPAND(ARRAY_AGG('||Range_Column||')) AS '||Range_Column||'
			FROM INPUT_TEMP1 
			'||v_Group_By||'
			INTERSECT
			SELECT '||v_Select_List||'TEMPORAL.EXPAND(ARRAY_AGG('||Range_Column||')) AS '||Range_Column||'
			FROM INPUT_TEMP2
			'||v_Group_By||'
		) SUB
		'||v_Group_By||'';
		DROP TABLE INPUT_TEMP1;
		DROP TABLE INPUT_TEMP2;
		RETURN;
        END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION TEMPORAL.TEMPORAL_JOIN (
	In_Statement1 TEXT, 
	In_Statement2 TEXT, 
	In_Grouping_Columns TEXT,
	In_Join_Condition TEXT,
	In_Range_Column TEXT
) RETURNS SETOF RECORD AS $$
	DECLARE 
		v_Rec RECORD;
		v_Select_List TEXT;
		v_Group_By TEXT;
	BEGIN
		PERFORM TEMPORAL.PREPARE_TEMP(In_Statement1, 1); --INPUT_TEMP1
		PERFORM TEMPORAL.PREPARE_TEMP(In_Statement2, 2); --INPUT_TEMP2
		
		IF In_Grouping_Columns IS NULL OR CHAR_LENGTH(In_Grouping_Columns)=0 THEN 
			v_Select_List := '';
			v_Group_By := '';
		ELSE
			v_Select_List := In_Grouping_Columns||', ';
			v_Group_By := 'GROUP BY '||In_Grouping_Columns;
		END IF;

		RETURN QUERY 
		EXECUTE format('
		SELECT '||v_Select_List||'TEMPORAL.COLLAPSE(ARRAY_AGG('||In_Range_Column||')) AS '||In_Range_Column||'
		FROM (
			SELECT *
			FROM (
				SELECT '||v_Select_List||'TEMPORAL.EXPAND(ARRAY_AGG('||In_Range_Column||')) AS '||In_Range_Column||'
				FROM INPUT_TEMP1 
				'||v_Group_By||'
			) AS S1
			JOIN (
				SELECT '||v_Select_List||'TEMPORAL.EXPAND(ARRAY_AGG('||In_Range_Column||')) AS '||In_Range_Column||'
				FROM INPUT_TEMP2
				'||v_Group_By||'
			) AS S2
				ON 
		) SUB
		'||v_Group_By||'');
		DROP TABLE INPUT_TEMP1;
		DROP TABLE INPUT_TEMP2;
		RETURN;
        END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION TEMPORAL.TEMPORAL_MINUS(
	Statement1 TEXT, 
	Statement2 TEXT, 
	Grouping_Columns TEXT,
	Range_Column VARCHAR(63)
) RETURNS SETOF RECORD AS $$
	DECLARE 
		v_Rec RECORD;
		v_Select_List TEXT;
		v_Group_By TEXT;
	BEGIN
		PERFORM TEMPORAL.PREPARE_TEMP(Statement1, 1); --INPUT_TEMP1
		PERFORM TEMPORAL.PREPARE_TEMP(Statement2, 2); --INPUT_TEMP2
		
		IF Grouping_Columns IS NULL OR CHAR_LENGTH(Grouping_Columns)=0 THEN 
			v_Select_List := '';
			v_Group_By := '';
		ELSE
			v_Select_List := Grouping_Columns||', ';
			v_Group_By := 'GROUP BY '||Grouping_Columns;
		END IF;

		RETURN QUERY 
		EXECUTE '
		SELECT '||v_Select_List||'TEMPORAL.COLLAPSE(ARRAY_AGG('||Range_Column||')) AS '||Range_Column||'
		FROM (
			SELECT '||v_Select_List||'TEMPORAL.EXPAND(ARRAY_AGG('||Range_Column||')) AS '||Range_Column||'
			FROM INPUT_TEMP1 
			'||v_Group_By||'
			EXCEPT
			SELECT '||v_Select_List||'TEMPORAL.EXPAND(ARRAY_AGG('||Range_Column||')) AS '||Range_Column||'
			FROM INPUT_TEMP2
			'||v_Group_By||'
		) SUB
		'||v_Group_By||'';
		DROP TABLE INPUT_TEMP1;
		DROP TABLE INPUT_TEMP2;
		RETURN;
        END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION TEMPORAL.TEMPORAL_UNION(
	Statement1 TEXT, 
	Statement2 TEXT, 
	Grouping_Columns TEXT,
	Range_Column VARCHAR(63)
) RETURNS SETOF RECORD AS $$
	DECLARE 
		v_Rec RECORD;
		v_Select_List TEXT;
		v_Group_By TEXT;
	BEGIN
		PERFORM TEMPORAL.PREPARE_TEMP(Statement1, 1); --INPUT_TEMP1
		PERFORM TEMPORAL.PREPARE_TEMP(Statement2, 2); --INPUT_TEMP2

		IF Grouping_Columns IS NULL OR CHAR_LENGTH(Grouping_Columns)=0 THEN 
			v_Select_List := '';
			v_Group_By := '';
		ELSE
			v_Select_List := Grouping_Columns||', ';
			v_Group_By := 'GROUP BY '||Grouping_Columns;
		END IF;

		RETURN QUERY 
		EXECUTE '
		SELECT '||v_Select_List||'TEMPORAL.COLLAPSE(ARRAY_AGG('||Range_Column||')) AS '||Range_Column||'
		FROM (
			SELECT '||v_Select_List||Range_Column||' AS '||Range_Column||'
			FROM INPUT_TEMP1 
			UNION
			SELECT '||v_Select_List||Range_Column||' AS '||Range_Column||'
			FROM INPUT_TEMP2
		) SUB
		'||v_Group_By||'';
		DROP TABLE INPUT_TEMP1;
		DROP TABLE INPUT_TEMP2;
		RETURN;
        END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION TEMPORAL.CHECK_SCHEMA_EXISTS (
	In_Schema_Name TEXT
)
RETURNS VOID AS $$
BEGIN
	IF NOT EXISTS (
		SELECT 1 
		FROM PG_CATALOG.PG_NAMESPACE 
		WHERE NspName=LOWER(In_Schema_Name)
	) THEN RAISE EXCEPTION 'Schema % does not exist', In_Schema_Name;
	END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION TEMPORAL.CHECK_TABLE_EXISTS (
	In_Schema_Name TEXT,
	In_Table_Name TEXT
)
RETURNS VOID AS $$
BEGIN
	IF NOT EXISTS (
		SELECT 1 
		FROM PG_CATALOG.PG_NAMESPACE AS SCH
		JOIN PG_CATALOG.PG_CLASS AS TAB
			ON TAB.RelNamespace=SCH.Oid
		WHERE SCH.NspName=LOWER(In_Schema_Name)
		AND TAB.relname=LOWER(In_Table_Name)
	) THEN RAISE EXCEPTION 'Table %.% does not exist', In_Schema_Name, In_Table_Name;
	END IF;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION TEMPORAL.CREATE_ATTRIBUTE_VIEW (
	In_Schema_Name TEXT,
	In_Combining_View_Name TEXT,
	In_Table_Name TEXT,
	In_Hist_Table_Name TEXT,
	In_Hist_Col_List TEXT,
	In_Since_Name TEXT
) RETURNS VOID AS $$
BEGIN
	EXECUTE format('
		CREATE OR REPLACE VIEW %s.%s AS (
			SELECT %s, CAST(''[''||%s||'',INFINITY)'' AS DATERANGE) AS DURING
			FROM %s.%s
			UNION
			SELECT %s, DURING
			FROM %s.%s
		);', In_Schema_Name, In_Combining_View_Name, In_Hist_Col_List, In_Since_Name, In_Schema_Name, In_Table_Name, In_Hist_Col_List, In_Schema_Name, In_Hist_Table_Name
	);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION TEMPORAL.CREATE_FULL_VIEW (
	In_Schema_Name TEXT,
	In_Table_Name TEXT
) RETURNS VOID AS $$
	DECLARE
		v_Record RECORD;
		v_PK_Column_List TEXT;
		v_Statement TEXT;
		v_Join_Statements TEXT;
		v_Loop_Index SMALLINT;
		v_Property_Column_List TEXT;
		v_Final_Column_List TEXT;
		v_Combining_View_Name TEXT;
		v_Join_Condition TEXT;
		v_Join TEXT;
		v_View_Name TEXT;
	BEGIN
		v_Loop_Index:=1;
		v_View_Name:=format('%s.%s_full_vw', In_Schema_Name, In_Table_Name);
		v_Statement:=format('DROP VIEW IF EXISTS %s; CREATE VIEW %s AS (', v_View_Name, v_View_Name);
		v_PK_Column_List:='';
		v_Property_Column_List:='';
		v_Join_Statements:='';
		v_Join:='';

		CREATE TEMPORARY TABLE PK_AND_JOIN AS (
			SELECT 
				PK_Column_List, 
				' ON '||ARRAY_TO_STRING(ARRAY_AGG('s1.'||Join_Member||'=s2.'||Join_Member||' AND '),'')||'s1.DURING=s2.DURING ' AS Join_Condition
			FROM (
				SELECT 
					ARRAY_TO_STRING(Property_Column_List,',') AS PK_Column_List,
					UNNEST(Property_Column_List) AS Join_Member
				FROM TEMPORAL.TEMPORAL_METADATA
				WHERE LOWER(Schema_Name)=In_Schema_Name
				AND LOWER(Table_Name)=In_Table_Name
				AND Is_Property_Pk IS TRUE
			) SUB
			GROUP BY PK_Column_List
		);
		v_PK_Column_List:=(SELECT PK_Column_List FROM PK_AND_JOIN);
		v_Final_Column_List:='T1.'||REPLACE(v_PK_Column_List,',',',T1.');

		FOR v_Record IN
			SELECT 
				CASE WHEN Is_Property_Pk THEN '' ELSE ','||ARRAY_TO_STRING(Property_Column_List,',') END AS Property_Column_List, 
				Combining_View_Name
			FROM TEMPORAL.TEMPORAL_METADATA
			WHERE LOWER(Schema_Name)=In_Schema_Name
			AND LOWER(Table_Name)=In_Table_Name
			ORDER BY Is_Property_Pk
		LOOP
			v_Property_Column_List:=v_PK_Column_List||v_Record.Property_Column_List;
			v_Final_Column_List:=v_Final_Column_List||v_Record.Property_Column_List;
			v_Combining_View_Name:=v_Record.Combining_View_Name;

			IF v_Loop_Index>1 THEN
				v_Join:=' JOIN ';
				v_Join_Condition:=(SELECT Join_Condition FROM PK_AND_JOIN);
				v_Join_Condition:=REPLACE(v_Join_Condition,'s1.','T'||v_Loop_Index-1||'.');
				v_Join_Condition:=REPLACE(v_Join_Condition,'s2.','T'||v_Loop_Index||'.');
			END IF;
			
			v_Join_Statements:=v_Join_Statements||format(
				'%s (SELECT %s, TEMPORAL.EXPAND(ARRAY_AGG(DURING)) AS DURING
				FROM %I.%I
				GROUP BY %s) AS T%s
				', v_Join,v_Property_Column_List, In_Schema_Name, v_Combining_View_Name,v_Property_Column_List, v_Loop_Index
			);
			IF v_Loop_Index>1 THEN
				v_Join_Statements:=v_Join_Statements||v_Join_Condition;
			END IF;

			v_Loop_Index:=v_Loop_Index+1;
		END LOOP;

		v_Statement:=v_Statement||format('
		SELECT %s, 
		TEMPORAL.COLLAPSE(ARRAY_AGG(
			CASE WHEN TEMPORAL.END(T1.DURING)=CURRENT_DATE THEN CAST(''[''||TEMPORAL.BEGIN(T1.DURING)||'',INFINITY)'' AS DATERANGE) 
			ELSE T1.DURING END)
		) AS DURING 
		FROM %s ', v_Final_Column_List, v_Join_Statements);
		v_Statement:=v_Statement||format('GROUP BY %s);', v_Final_Column_List);

		EXECUTE format(v_Statement);
		DROP TABLE PK_AND_JOIN;
    END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION TEMPORAL.PREPARE_TEMP(SQL_Statement TEXT, Idx INTEGER) RETURNS VOID AS $$
	BEGIN
		EXECUTE 'CREATE TEMPORARY TABLE INPUT_TEMP'||Idx||' AS (' || SQL_Statement || ');';
        END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION TEMPORAL.PREP_TMP_FK_TABLE(In_Schema_Name TEXT, In_Table_Name TEXT) RETURNS VOID AS $$
BEGIN
	CREATE TEMPORARY TABLE TMP_FK_METADATA AS (
		SELECT
			KCU.Column_Name, 
			CCU.Table_Schema AS Foreign_Schema_Name,
			CCU.Table_Name AS Foreign_Table_Name,
			CCU.Column_Name AS Foreign_Column_Name,
			META.Property_Column_List,
			META.Since_Column_Name,
			META.Hist_Table_Name
		FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS AS TC 
		JOIN INFORMATION_SCHEMA.Key_Column_Usage AS KCU
			ON TC.Constraint_Name =  KCU.Constraint_Name
		JOIN INFORMATION_SCHEMA.Constraint_Column_Usage AS CCU
			ON CCU.Constraint_Name = TC.Constraint_Name
		JOIN TEMPORAL.TEMPORAL_METADATA AS META
			ON META.Schema_Name=CCU.Table_Schema
			AND META.Table_Name=CCU.Table_Name
			AND META.Is_Property_PK = TRUE
		WHERE Constraint_Type = 'FOREIGN KEY' 
		AND TC.Table_Schema=LOWER(In_Schema_Name)
		AND TC.Table_Name=LOWER(In_Table_Name)
	);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION TEMPORAL.PREP_TMP_PG_CATALOG (In_Schema_Name TEXT, In_Table_Name TEXT, In_Columns_Names TEXT)
RETURNS VOID AS $$
DECLARE
	v_Schema_Name TEXT;
	v_Table_Name TEXT;
	v_Columns_Names TEXT;
BEGIN
	v_Schema_Name=LOWER(TRIM(In_Schema_Name));
	v_Table_Name=LOWER(TRIM(In_Table_Name));
	v_Columns_Names=LOWER(TRIM(In_Columns_Names));

	CREATE TEMPORARY TABLE TMP_HIST_TAB_COLUMN AS (
		SELECT 
			COL.AttName AS Column_Name,
			COL.AttNum,
			DATATYPE.TypName AS Column_Type,
			MAX(CASE
				WHEN TAB_CONSTRAINT.ConType='p' AND COL.AttNum=ANY(TAB_CONSTRAINT.ConKey) THEN 1
				ELSE 0
			END) AS Is_PK_Column,
			MAX(CASE 
				WHEN COL.AttNotNull IS TRUE THEN 'NOT NULL'
				ELSE ''
			END) AS Nullable,
			ARRAY_AGG(TAB_CONSTRAINT.confrelid) AS FK_Ref_Tables,
			ARRAY_AGG(ARRAY_TO_STRING(TAB_CONSTRAINT.confkey, ',')) AS FK_Ref_Columns
		FROM PG_CATALOG.PG_NAMESPACE AS SCH
		JOIN PG_CATALOG.PG_CLASS AS TAB
			ON TAB.RelNamespace=SCH.Oid
		JOIN PG_CATALOG.PG_ATTRIBUTE AS COL
			ON COL.AttRelId=TAB.Oid
		JOIN PG_CATALOG.PG_TYPE AS DATATYPE
			ON COL.AttTypId=DATATYPE.Oid
		JOIN PG_CATALOG.PG_CONSTRAINT AS TAB_CONSTRAINT
			ON TAB_CONSTRAINT.ConNamespace=SCH.Oid
			AND TAB_CONSTRAINT.ConRelId=TAB.Oid
			AND TAB_CONSTRAINT.ConType IN ('p') /*Primary foreign key constraint*/
		WHERE 
			SCH.NspName=LOWER(v_Schema_Name)
			AND TAB.relname=LOWER(v_Table_Name)
			AND (
				COL.AttNum=ANY(TAB_CONSTRAINT.ConKey) /*PK cols*/
				OR COL.AttName IN (v_Columns_Names) /*Property cols*/
			)
		GROUP BY 
			COL.AttName,
			COL.AttNum,
			DATATYPE.TypName
	)	
	;
END;
$$ LANGUAGE plpgsql;

/*
EXAMPLE: SELECT TEMPORAL.TEMPORALIZE ('Temporal','Customer','Customer_Name','Cust_Name_Since','CUSTOMER_HIST')
*/
CREATE OR REPLACE FUNCTION TEMPORAL.TEMPORALIZE (
	In_Schema_Name TEXT, 
	In_Table_Name TEXT,
	In_Columns_Names TEXT,
	In_Since_Name TEXT, 
	In_Hist_Table_Name TEXT,
	In_Combining_View_Name TEXT 
)
RETURNS VOID AS $$
DECLARE
	v_Schema_Name TEXT;
	v_Table_Name TEXT;
	v_Columns_Names TEXT;
	v_Since_Name TEXT;
	v_Hist_Table_Name TEXT;
	v_Combining_View_Name TEXT;
	v_Record RECORD;
	v_Hist_Col_List TEXT;
	v_Col_DType_Nullable TEXT;
	v_PK_Col_List TEXT;
	v_Property_Is_PK BOOLEAN;
	v_PK_Since_Col TEXT;
	v_Main_PK_Col_List TEXT;
	v_Property_Col_FK_Member BOOLEAN;
	v_Main_DURING_Table_Name TEXT;
	v_Main_SINCE_Table_Name TEXT;
	v_Main_Schema_Name TEXT;
	v_Main_FK_PK_Col_List TEXT;
	v_Main_Hist_Table_Name TEXT;
	v_Main_Since_Column_Name TEXT;
BEGIN
	v_Schema_Name=LOWER(REPLACE(In_Schema_Name,' ',''));
	v_Table_Name=LOWER(REPLACE(In_Table_Name,' ',''));
	v_Columns_Names=LOWER(REPLACE(In_Columns_Names,' ',''));
	v_Since_Name=LOWER(REPLACE(In_Since_Name,' ',''));
	v_Hist_Table_Name=LOWER(REPLACE(In_Hist_Table_Name,' ',''));
	v_Combining_View_Name=LOWER(REPLACE(In_Combining_View_Name,' ',''));

	PERFORM TEMPORAL.CHECK_SCHEMA_EXISTS(v_Schema_Name);
	PERFORM TEMPORAL.CHECK_TABLE_EXISTS(v_Schema_Name, v_Table_Name);
	PERFORM TEMPORAL.PREP_TMP_PG_CATALOG (v_Schema_Name, v_Table_Name, v_Columns_Names);

	SELECT 
		ARRAY_TO_STRING(ARRAY_AGG(Col_Data_Type_And_Nullable), ',')||', DURING DATERANGE NOT NULL',
		ARRAY_TO_STRING(ARRAY_AGG(PK_Cols_Member), ',')||', DURING',
		ARRAY_TO_STRING(ARRAY_AGG(All_Cols_Member), ',')=ARRAY_TO_STRING(ARRAY_AGG(PK_Cols_Member), ','),
		ARRAY_TO_STRING(ARRAY_AGG(All_Cols_Member), ','),
		ARRAY_TO_STRING(ARRAY_AGG(PK_Cols_Member), ',')
	FROM (
		SELECT 
			Column_Name||' '||Column_Type||' '||Nullable AS Col_Data_Type_And_Nullable,
			CASE WHEN Is_PK_Column =1 THEN Column_Name ELSE NULL END AS PK_Cols_Member,
			Column_Name AS All_Cols_Member
		FROM TMP_HIST_TAB_COLUMN S
		ORDER BY AttNum
	) SUB
	INTO v_Col_DType_Nullable, v_PK_Col_List, v_Property_Is_PK, v_Hist_Col_List, v_Main_PK_Col_List
	;

	EXECUTE format('
		CREATE TABLE %I.%I (
			%s,
			CONSTRAINT PK_%s PRIMARY KEY (%s, DURING),
			CONSTRAINT %s_chk_DURING_not_empty CHECK (DURING<>''Empty'')
		);
	', v_Schema_Name, v_Hist_Table_Name, v_Col_DType_Nullable, In_Hist_Table_Name, v_Main_PK_Col_List, In_Hist_Table_Name);
		

	INSERT INTO TEMPORAL.TEMPORAL_METADATA (
		Schema_Name,
		Table_Name, 
		Property_Column_List, 
		Is_Property_PK, 
		Since_Column_Name, 
		Hist_Table_Name,
		Combining_View_Name
	) VALUES (
		v_Schema_Name,
		v_Table_Name,
		STRING_TO_ARRAY(v_Columns_Names, ','),
		v_Property_Is_PK,
		v_Since_Name,
		v_Hist_Table_Name,
		v_Combining_View_Name
	);

	--PACK ON constraint
	EXECUTE format('
		CREATE CONSTRAINT TRIGGER %s_CHK_PACKED_ON
		AFTER INSERT OR UPDATE OR DELETE ON %I.%I
		DEFERRABLE INITIALLY DEFERRED
		FOR EACH ROW EXECUTE PROCEDURE TEMPORAL.CHK_PACKED_ON_DURING(''%s'');
	',v_Hist_Table_Name, v_Schema_Name, v_Hist_Table_Name, v_Hist_Col_List);

	--"WHEN UNPACKED THEN KEY" constraint
	EXECUTE format('
		CREATE CONSTRAINT TRIGGER %s_CHK_WHEN_THEN
		AFTER INSERT OR UPDATE OR DELETE ON %I.%I
		DEFERRABLE INITIALLY DEFERRED
		FOR EACH ROW EXECUTE PROCEDURE TEMPORAL.CHK_WHEN_UNPACKED_THEN_KEY(''%s'',''%s'', ''%s'');
	',v_Hist_Table_Name, v_Schema_Name, v_Hist_Table_Name, v_Hist_Col_List, v_Main_PK_Col_List, v_Columns_Names);

	EXECUTE format('
		CREATE CONSTRAINT TRIGGER CHK_%s_NO_REDUNDANCY_WITH_DURING
		AFTER INSERT OR UPDATE OR DELETE ON %I.%I
		DEFERRABLE INITIALLY DEFERRED
		FOR EACH ROW EXECUTE PROCEDURE TEMPORAL.CHK_NO_REDUNDANCY_ACROSS_SINCE_AND_DURING (%s, %s, %s, %s);
	', v_Since_Name, v_Schema_Name, v_Table_Name, v_Table_Name, v_Since_Name, v_Hist_Table_Name, v_Main_PK_Col_List);

	EXECUTE format('
		CREATE CONSTRAINT TRIGGER CHK_NO_REDUNDANCY_WITH_%s
		AFTER INSERT OR UPDATE OR DELETE ON %I.%I
		DEFERRABLE INITIALLY DEFERRED
		FOR EACH ROW EXECUTE PROCEDURE TEMPORAL.CHK_NO_REDUNDANCY_ACROSS_SINCE_AND_DURING (%s, %s, %s, %s);
	', v_Since_Name, v_Schema_Name, v_Hist_Table_Name, v_Table_Name, v_Since_Name, v_Hist_Table_Name, v_Main_PK_Col_List);

	EXECUTE format('
		CREATE CONSTRAINT TRIGGER CHK_%s_NO_CIRCUMLOCUTION_WITH_DURING
		AFTER INSERT OR UPDATE OR DELETE ON %I.%I
		DEFERRABLE INITIALLY DEFERRED
		FOR EACH ROW EXECUTE PROCEDURE TEMPORAL.CHK_NO_CIRCUMLOCUTION_ACROSS_SINCE_AND_DURING (%s, %s, %s, %s);
	', v_Since_Name, v_Schema_Name, v_Table_Name, v_Table_Name, v_Since_Name, v_Hist_Table_Name, v_Hist_Col_List);

	EXECUTE format('
		CREATE CONSTRAINT TRIGGER NO_CIRCUMLOCUTION_WITH_%s
		AFTER INSERT OR UPDATE OR DELETE ON %I.%I
		DEFERRABLE INITIALLY DEFERRED
		FOR EACH ROW EXECUTE PROCEDURE TEMPORAL.CHK_NO_CIRCUMLOCUTION_ACROSS_SINCE_AND_DURING (%s, %s, %s, %s);
	', v_Since_Name, v_Schema_Name, v_Hist_Table_Name, v_Table_Name, v_Since_Name, v_Hist_Table_Name, v_Hist_Col_List);

	IF v_Property_Is_PK IS FALSE THEN
		SELECT Hist_Table_Name, Since_Column_Name 
		INTO v_Main_DURING_Table_Name, v_PK_Since_Col
		FROM TEMPORAL.TEMPORAL_METADATA
		WHERE LOWER(Schema_Name)=v_Schema_Name 
		AND LOWER(Table_Name)=v_Table_Name 
		AND Is_Property_PK IS TRUE;

		IF v_PK_Since_Col IS NULL THEN 
			RAISE EXCEPTION 'SINCE column for PRIMARY KEY is not registered in TEMPORAL_METADATA';
		END IF;

		EXECUTE format('
			ALTER TABLE %I.%I
			ADD CONSTRAINT %s_not_later_than_PK_Since
			CHECK (%I<=%I);
		', v_Schema_Name, v_Table_Name, v_Since_Name, v_PK_Since_Col, v_Since_Name);

		--- since table
		EXECUTE format('
			CREATE CONSTRAINT TRIGGER CHK_%s_Integrity
			AFTER INSERT OR UPDATE OR DELETE ON %I.%I
			DEFERRABLE INITIALLY DEFERRED
			FOR EACH ROW EXECUTE PROCEDURE TEMPORAL.CHK_ATTRIBUTE_TEMPORAL_INTEGRITY (%s, %s, %s, %s, ''%s'', %s);
		', v_Since_Name, v_Schema_Name, v_Table_Name
			, v_Table_Name, v_Since_Name, v_Hist_Table_Name, v_Main_DURING_Table_Name, v_Main_PK_Col_List, v_PK_Since_Col);

		-- during table
		EXECUTE format('
			CREATE CONSTRAINT TRIGGER CHK_%s_Integrity
			AFTER INSERT OR UPDATE OR DELETE ON %I.%I
			DEFERRABLE INITIALLY DEFERRED
			FOR EACH ROW EXECUTE PROCEDURE TEMPORAL.CHK_ATTRIBUTE_TEMPORAL_INTEGRITY (%s, %s, %s, %s, ''%s'', %s);
		', v_Since_Name, v_Schema_Name, v_Hist_Table_Name
			, v_Table_Name, v_Since_Name, v_Hist_Table_Name, v_Main_DURING_Table_Name, v_Main_PK_Col_List, v_PK_Since_Col);
		
		-- main during table
		EXECUTE format('
			CREATE CONSTRAINT TRIGGER CHK_%s_Integrity
			AFTER INSERT OR UPDATE OR DELETE ON %I.%I
			DEFERRABLE INITIALLY DEFERRED
			FOR EACH ROW EXECUTE PROCEDURE TEMPORAL.CHK_ATTRIBUTE_TEMPORAL_INTEGRITY (%s, %s, %s, %s, ''%s'', %s);
		', v_Since_Name, v_Schema_Name, v_Main_DURING_Table_Name
			, v_Table_Name, v_Since_Name, v_Hist_Table_Name, v_Main_DURING_Table_Name, v_Main_PK_Col_List, v_PK_Since_Col);
	END IF;

	PERFORM TEMPORAL.PREP_TMP_FK_TABLE(v_Schema_Name, v_Table_Name);
	IF (
		SELECT COUNT(*) C FROM (
			SELECT Foreign_Column_Name, UNNEST(Property_Column_List) AS Temporal_Meta_Column
			FROM TMP_FK_METADATA
		) SUB
		WHERE Foreign_Column_Name=Temporal_Meta_Column
	)>0 THEN
		
		EXECUTE format('
			CREATE CONSTRAINT TRIGGER CHK_%s_IN_FK_Since
			AFTER INSERT OR UPDATE OR DELETE ON %I.%I
			DEFERRABLE INITIALLY DEFERRED
			FOR EACH ROW EXECUTE PROCEDURE TEMPORAL.CHK_SINCE_IN_FK_SINCE (%s);
		', v_Since_Name, v_Schema_Name, v_Table_Name, v_Since_Name);

		SELECT DISTINCT Foreign_Schema_Name, Foreign_Table_Name, Since_Column_Name, Hist_Table_Name, ARRAY_TO_STRING(Property_Column_List, ',')
		INTO v_Main_Schema_Name, v_Main_SINCE_Table_Name, v_Main_Since_Column_Name, v_Main_Hist_Table_Name, v_Main_FK_PK_Col_List
		FROM TMP_FK_METADATA;

		--during
		EXECUTE format('
			CREATE CONSTRAINT TRIGGER CHK_DURING_IN_FK
			AFTER INSERT OR UPDATE OR DELETE ON %I.%I
			DEFERRABLE INITIALLY DEFERRED
			FOR EACH ROW EXECUTE PROCEDURE TEMPORAL.CHK_FK_TEMPORAL_INTEGRITY (%s,%s,%s,%s,%s,''%s'');
		', v_Schema_Name, v_Hist_Table_Name
			,v_Hist_Table_Name, v_Main_Schema_Name, v_Main_SINCE_Table_Name, v_Main_Since_Column_Name, v_Main_Hist_Table_Name, v_Main_FK_PK_Col_List);

		--main since
		EXECUTE format('
			CREATE CONSTRAINT TRIGGER CHK_DURING_IN_FK
			AFTER INSERT OR UPDATE OR DELETE ON %I.%I
			DEFERRABLE INITIALLY DEFERRED
			FOR EACH ROW EXECUTE PROCEDURE TEMPORAL.CHK_FK_TEMPORAL_INTEGRITY (%s,%s,%s,%s,%s,''%s'');
		', v_Schema_Name, v_Main_SINCE_Table_Name
			,v_Hist_Table_Name, v_Main_Schema_Name, v_Main_SINCE_Table_Name, v_Main_Since_Column_Name, v_Main_Hist_Table_Name, v_Main_FK_PK_Col_List);

		----main during
		EXECUTE format('
			CREATE CONSTRAINT TRIGGER CHK_DURING_IN_FK
			AFTER INSERT OR UPDATE OR DELETE ON %I.%I
			DEFERRABLE INITIALLY DEFERRED
			FOR EACH ROW EXECUTE PROCEDURE TEMPORAL.CHK_FK_TEMPORAL_INTEGRITY (%s,%s,%s,%s,%s,''%s'');
		', v_Schema_Name, v_Main_Hist_Table_Name
			,v_Hist_Table_Name, v_Main_Schema_Name, v_Main_SINCE_Table_Name, v_Main_Since_Column_Name, v_Main_Hist_Table_Name, v_Main_FK_PK_Col_List);
		
	END IF;
	
	PERFORM TEMPORAL.CREATE_ATTRIBUTE_VIEW(v_Schema_Name, v_Combining_View_Name, v_Table_Name, v_Hist_Table_Name, v_Hist_Col_List, v_Since_Name);
	PERFORM TEMPORAL.CREATE_FULL_VIEW(v_Schema_Name, v_Table_Name);
	
	DROP TABLE TMP_HIST_TAB_COLUMN;
	DROP TABLE TMP_FK_METADATA;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION TEMPORAL.CHK_ATTRIBUTE_TEMPORAL_INTEGRITY() RETURNS TRIGGER AS $$
DECLARE
	v_Schema_Name TEXT;
	v_Main_DURING_Table_Name TEXT;
	v_Main_SINCE_Column_Name TEXT;
	v_SINCE_Table_Name TEXT;
	v_SINCE_Column_Name TEXT;
	v_DURING_Table_Name TEXT;
	v_Record RECORD;
	v_SINCE_Table_Rows TEXT;
	v_Main_PK_Col_List TEXT;
BEGIN
	v_Schema_Name := TG_TABLE_SCHEMA;

	v_SINCE_Table_Name := TG_ARGV[0];
	v_SINCE_Column_Name := TG_ARGV[1];
	v_DURING_Table_Name := TG_ARGV[2];
	v_Main_DURING_Table_Name := TG_ARGV[3];
	v_Main_PK_Col_List := TG_ARGV[4];
	v_Main_SINCE_Column_Name := TG_ARGV[5];

	--1.) select row values
	v_SINCE_Table_Rows:=format('
		SELECT %s, CAST(''[''||%I||'', ''||TEMPORAL.PRIOR_DATE(%I)||'']'' AS DATERANGE) AS DURING
		FROM %I.%I
		WHERE %I<%I
		', v_Main_PK_Col_List, v_Main_SINCE_Column_Name, v_SINCE_Column_Name, v_Schema_Name, v_SINCE_Table_Name, v_Main_SINCE_Column_Name, v_SINCE_Column_Name
	);
	
	--2.) select from main DURING table
	EXECUTE format(
		'CREATE TEMPORARY TABLE QUERY1 AS (SELECT %s, DURING FROM %I.%I AS T1 UNION %s)'
		, v_Main_PK_Col_List, v_Schema_Name, v_Main_DURING_Table_Name, v_SINCE_Table_Rows
	);

	--3.) select from property DURING table
	EXECUTE format(
		'CREATE TEMPORARY TABLE QUERY2 AS (SELECT %s, DURING FROM %I.%I AS T2)'
		, v_Main_PK_Col_List, v_Schema_Name, v_DURING_Table_Name
	);

	--4.) combine the 2 queries into function TEMPORAL_EQUALS to compare them
	--Check if the new SINCE would be less or equal than DURING in hist table
	IF (
		SELECT TEMPORAL.TEMPORAL_EQUALS(
		'SELECT * FROM QUERY1', 
		'SELECT * FROM QUERY2', 
		NULL, 
		'DURING'
		) AS Res
	) IS FALSE THEN
	    RAISE EXCEPTION 'Entity and its attribute have some gaps in their relationship: %.%', v_SINCE_Table_Name, v_SINCE_Column_Name;
	END IF;

	DROP TABLE QUERY1;
	DROP TABLE QUERY2;
	RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION TEMPORAL.CHK_FK_TEMPORAL_INTEGRITY() RETURNS TRIGGER AS $$
DECLARE
	v_Schema_Name TEXT;
	v_DURING_Table_Name TEXT;
	v_Main_Schema_Name TEXT;
	v_Main_SINCE_Table_Name TEXT;
	v_Main_SINCE_Column_Name TEXT;
	v_Main_DURING_Table_Name TEXT;
	v_Main_PK_Col_List TEXT;
	v_Main_SINCE_Table_Rows TEXT;
	v_Record RECORD;
BEGIN
	v_Schema_Name := TG_TABLE_SCHEMA;
	v_DURING_Table_Name := TG_ARGV[0];
	v_Main_Schema_Name := TG_ARGV[1];
	v_Main_SINCE_Table_Name := TG_ARGV[2];
	v_Main_SINCE_Column_Name := TG_ARGV[3];
	v_Main_DURING_Table_Name := TG_ARGV[4];
	v_Main_PK_Col_List := TG_ARGV[5];

	--1.) select from property DURING table
	EXECUTE format(
		'CREATE TEMPORARY TABLE QUERY1 AS (SELECT %s, DURING FROM %I.%I AS T2)'
		, v_Main_PK_Col_List, v_Schema_Name, v_DURING_Table_Name
	);

	--2.) select fact DURING values
	v_Main_SINCE_Table_Rows:=format('
		SELECT %s, CAST(''[''||%I||'', INFINITY)'' AS DATERANGE) AS DURING
		FROM %I.%I
		', v_Main_PK_Col_List, v_Main_SINCE_Column_Name, v_Schema_Name, v_Main_SINCE_Table_Name
	);
	
	--3.) select from main DURING table
	EXECUTE format(
		'CREATE TEMPORARY TABLE QUERY2 AS (SELECT %s, DURING FROM %I.%I AS T1 UNION %s)'
		, v_Main_PK_Col_List, v_Schema_Name, v_Main_DURING_Table_Name, v_Main_SINCE_Table_Rows
	);

	--4.) combine the 2 queries into function TEMPORAL_MINUS to compare them
	EXECUTE format('
		SELECT COUNT(*) AS Error_Cnt FROM (
			(
				SELECT SUB1.*
				FROM (
					SELECT %s, TEMPORAL.EXPAND(ARRAY_AGG(DURING)) AS DURING
					FROM QUERY1 
					GROUP BY %s
				) SUB1
			) EXCEPT (
				SELECT SUB2.*
				FROM (
					SELECT %s, TEMPORAL.EXPAND(ARRAY_AGG(DURING)) AS DURING
					FROM QUERY2
					GROUP BY %s
				) SUB2
			)
		) RES
	',v_Main_PK_Col_List,v_Main_PK_Col_List,v_Main_PK_Col_List,v_Main_PK_Col_List) INTO v_Record;

	IF v_Record.Error_Cnt>0 THEN
	    RAISE EXCEPTION 'Entity and its reference table have some gaps in their relationship: %', v_DURING_Table_Name;
	END IF;

	DROP TABLE QUERY1;
	DROP TABLE QUERY2;
	RETURN NULL;
END;
$$ LANGUAGE plpgsql;

/*
No redundancy is allowed across tables SINCE and DURING
*/
CREATE OR REPLACE FUNCTION TEMPORAL.CHK_NO_CIRCUMLOCUTION_ACROSS_SINCE_AND_DURING() RETURNS TRIGGER AS $$
DECLARE
	v_Schema_Name TEXT;
	v_DURING_Table_Name TEXT;
	v_SINCE_Table_Name TEXT;
	v_SINCE_Column_Name TEXT;

	v_Query_Statement TEXT;
	v_Join_Condition TEXT;
	v_Error_Cnt SMALLINT;
BEGIN
	v_Schema_Name := TG_TABLE_SCHEMA;

	v_SINCE_Table_Name := TG_ARGV[0];
	v_SINCE_Column_Name := TG_ARGV[1];
	v_DURING_Table_Name := TG_ARGV[2];

	v_Query_Statement:=format('SELECT COUNT(*) AS Error_Cnt FROM %I.%I AS T_SINCE', v_Schema_Name, v_SINCE_Table_Name);
	v_Query_Statement:=format('%s JOIN %I.%I AS T_DURING', v_Query_Statement, v_Schema_Name, v_DURING_Table_Name);
	v_Join_Condition:=' ON';

	FOR i IN 3..TG_NARGS-1 LOOP
		v_Join_Condition:=format('%s T_SINCE.%I=T_DURING.%I AND ', v_Join_Condition, TG_ARGV[i], TG_ARGV[i]);
	END LOOP;
	
	v_Join_Condition:=format('%s T_SINCE.%I=TEMPORAL.NEXT_DATE(TEMPORAL.END(DURING))', v_Join_Condition, v_SINCE_Column_Name);

	v_Query_Statement:=v_Query_Statement || v_Join_Condition;

	EXECUTE format(v_Query_Statement) INTO v_Error_Cnt;
	
	--Check if the new SINCE would be less or equal than DURING in hist table
	IF v_Error_Cnt>0  THEN
	    RAISE EXCEPTION 'No circumlocution is allowed across tables % and % (%.% must be later than %.DURATION end point+1 in case of equal attribute values)', v_SINCE_Table_Name, v_DURING_Table_Name, v_SINCE_Table_Name, v_SINCE_Column_Name, v_DURING_Table_Name;
	END IF;

	RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION TEMPORAL.CHK_NO_REDUNDANCY_ACROSS_SINCE_AND_DURING() RETURNS TRIGGER AS $$
DECLARE
	v_Schema_Name TEXT;
	v_DURING_Table_Name TEXT;
	v_SINCE_Table_Name TEXT;
	v_SINCE_Column_Name TEXT;

	v_Query_Statement TEXT;
	v_Join_Condition TEXT;
	v_Error_Cnt SMALLINT;
BEGIN
	v_Schema_Name := TG_TABLE_SCHEMA;

	v_SINCE_Table_Name := TG_ARGV[0];
	v_SINCE_Column_Name := TG_ARGV[1];
	v_DURING_Table_Name := TG_ARGV[2];

	v_Query_Statement:=format('SELECT COUNT(*) AS Error_Cnt FROM %I.%I AS T_SINCE', v_Schema_Name, v_SINCE_Table_Name);
	v_Query_Statement:=format('%s JOIN %I.%I AS T_DURING', v_Query_Statement, v_Schema_Name, v_DURING_Table_Name);
	v_Join_Condition:=' ON';

	FOR i IN 3..TG_NARGS-1 LOOP
		v_Join_Condition:=format('%s T_SINCE.%I=T_DURING.%I AND ', v_Join_Condition, TG_ARGV[i], TG_ARGV[i]);
	END LOOP;
	
	v_Join_Condition:=format('%s T_SINCE.%I<=TEMPORAL.END(DURING)', v_Join_Condition, v_SINCE_Column_Name);

	v_Query_Statement:=v_Query_Statement || v_Join_Condition;

	EXECUTE format(v_Query_Statement) INTO v_Error_Cnt;
	
	--Check if the new SINCE would be less or equal than DURING in hist table
	IF v_Error_Cnt>0  THEN
	    RAISE EXCEPTION 'No redundancy is allowed across tables % and % (%.% must be later than %.DURATION end point)', v_SINCE_Table_Name, v_DURING_Table_Name, v_SINCE_Table_Name, v_SINCE_Column_Name, v_DURING_Table_Name;
	END IF;

	RETURN NULL;
END;
$$ LANGUAGE plpgsql;


CREATE OR REPLACE FUNCTION TEMPORAL.CHK_PACKED_ON_DURING() RETURNS TRIGGER AS $$
DECLARE
	v_Query_Statement TEXT;
	v_Distinct_Col_List TEXT;
	v_Error_Cnt SMALLINT;
BEGIN
	v_Distinct_Col_List:=TG_ARGV[0];
	v_Query_Statement:=format('SELECT %s, DURING FROM %I.%I AS T1 EXCEPT ', v_Distinct_Col_List, TG_TABLE_SCHEMA,TG_TABLE_NAME);
	v_Query_Statement:=v_Query_Statement||format(
		'SELECT %s, TEMPORAL.COLLAPSE(ARRAY_AGG(DURING)) 
		FROM %I.%I AS T2
		GROUP BY %s', v_Distinct_Col_List, TG_TABLE_SCHEMA, TG_TABLE_NAME, v_Distinct_Col_List
	);


	EXECUTE format('SELECT COUNT(*) C FROM (%s) SUB;',v_Query_Statement) INTO v_Error_Cnt;

		IF v_Error_Cnt>0  THEN
		    RAISE EXCEPTION 'If at any given time table %.% contains two distinct rows that are identical except for their DURING values il and i2, then il MERGES i2 must be false', TG_TABLE_SCHEMA, TG_TABLE_NAME;
		END IF;

	RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION TEMPORAL.CHK_SINCE_IN_FK_SINCE() RETURNS TRIGGER AS $$
DECLARE
	v_Property_SINCE_Column_Name TEXT;
	v_Schema_Name TEXT;
	v_Table_Name TEXT;
	v_Columns_Names TEXT;
	v_Record RECORD;
	v_Error_Cnt SMALLINT;
BEGIN
	v_Schema_Name := TG_TABLE_SCHEMA;
	v_Table_Name := TG_TABLE_NAME;
	v_Property_SINCE_Column_Name := TG_ARGV[0];
	
	PERFORM TEMPORAL.PREP_TMP_FK_TABLE(v_Schema_Name, v_Table_Name);
	
	SELECT 
		FK_Schema,
		FK_Table,
		ARRAY_TO_STRING(ARRAY_AGG(Join_Condition), ' ')||' AND '||FK_Since_Col_Name AS Join_Condition
	INTO v_Record
	FROM (
		SELECT 
			Foreign_Schema_Name AS FK_Schema,
			Foreign_Table_Name AS FK_Table, 
			FK_Since_Col_Name, 
			'AND T1.'||Foreign_Column_Name||'=T2.'||Column_Name AS Join_Condition
		FROM (
			SELECT 
				Column_Name, 
				Foreign_Schema_Name, 
				Foreign_Table_Name, 
				Foreign_Column_Name, 
				UNNEST(Property_Column_List) AS Temporal_Meta_Column, 
				Since_Column_Name AS FK_Since_Col_Name
			FROM TEMPORAL.TMP_FK_METADATA
			WHERE Column_Name=Foreign_Column_Name
		) SUB
		WHERE Foreign_Column_Name=Temporal_Meta_Column
	) SUB1
	GROUP BY FK_Schema, FK_Table, FK_Since_Col_Name;
	
	EXECUTE format(
		'SELECT COUNT(*)
		FROM %I.%I AS T1 JOIN %I.%I AS T2
		ON (1=1) %s>T1.%s', v_Schema_Name, v_Table_Name, v_Record.FK_Schema, v_Record.FK_Table, v_Record.Join_Condition, v_Property_SINCE_Column_Name
	) INTO v_Error_Cnt;

	IF v_Error_Cnt>0 THEN
		RAISE EXCEPTION 'Property cannot exist before SINCE value of referenced table: %', v_Record.FK_Table;
	END IF;
	DROP TABLE TMP_FK_METADATA;
	RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION TEMPORAL.CHK_WHEN_UNPACKED_THEN_KEY() RETURNS TRIGGER AS $$
DECLARE
	v_Query_Statement TEXT;
	v_Distinct_Col_List TEXT;
	v_Main_PK_Col_List TEXT;
	v_Attribute_Name TEXT;
	v_Error_Cnt SMALLINT;
BEGIN
	v_Distinct_Col_List:=TG_ARGV[0];
	v_Main_PK_Col_List:=TG_ARGV[1];
	v_Attribute_Name:=TG_ARGV[2];
	v_Query_Statement:=format(
		'SELECT %s, TEMPORAL.EXPAND(ARRAY_AGG(DURING)) AS DURING
		FROM %I.%I
		GROUP BY %s', v_Distinct_Col_List, TG_TABLE_SCHEMA, TG_TABLE_NAME, v_Distinct_Col_List
	);


	EXECUTE format('
		SELECT COUNT(*) AS Error_Cnt FROM (
			SELECT %s, DURING, COUNT(*) C 
			FROM (%s) SUB 
			GROUP BY %s, DURING 
			HAVING COUNT(*)>1
		) SUB1',v_Main_PK_Col_List, v_Query_Statement,v_Main_PK_Col_List) INTO v_Error_Cnt;

		IF v_Error_Cnt>0  THEN
		    RAISE EXCEPTION 'If at any given time table %.% contains two distinct rows that are identical except for their % and DURING value, then their DURING values il and i2 must be such that il OVERLAPS i2 is false', TG_TABLE_SCHEMA, TG_TABLE_NAME, v_Attribute_Name;
		END IF;

	RETURN NULL;
END;
$$ LANGUAGE plpgsql;
