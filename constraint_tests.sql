/*
Run these delete statements before each test:
BEGIN;
DELETE FROM FTE.CUSTOMER WHERE Customer_Id=1 AND Country_Code=1;
DELETE FROM FTE.CUSTOMER_HIST WHERE Customer_Id=1 AND Country_Code=1;
DELETE FROM FTE.CUSTOMER_SEGMENT_HIST WHERE Customer_Id=1 AND Country_Code=1;
DELETE FROM FTE.CUSTOMER_NAME_HIST WHERE Customer_Id=1 AND Country_Code=1;
DELETE FROM FTE.CUSTOMER_CONTRACT_HIST WHERE Customer_Id=1 AND Country_Code=1;
COMMIT;

/*Test_Chk_Packed_On - when these three statements are executed in the given order one by one then the second and third statement should fail with an error "If at any given time table fte.customer_name_hist contains two distinct rows that are identical except for their DURING values il and i2, then il MERGES i2 must be false"*/
BEGIN;
INSERT INTO FTE.CUSTOMER_HIST (Customer_Id, Country_Code, DURING)
VALUES (1, 1, '[2015-01-01, 2015-02-01)');
INSERT INTO FTE.CUSTOMER_SEGMENT_HIST (Customer_Id, Country_Code, Customer_Segment_Code, DURING)
VALUES (1, 1, 1,'[2015-01-01, 2015-02-01)');
INSERT INTO FTE.CUSTOMER_NAME_HIST (Customer_Id, Country_Code, Customer_Name, DURING)
VALUES (1, 1, 'Mari Tamm', '[2015-01-01, 2015-02-01)');
COMMIT;
--an attempt to produce redundancy
INSERT INTO FTE.CUSTOMER_NAME_HIST (Customer_Id, Country_Code, Customer_Name, DURING)
VALUES (1, 1, 'Mari Tamm', '[2015-01-30, 2015-02-01)');
--an attempt to produce circumlocution
INSERT INTO FTE.CUSTOMER_NAME_HIST (Customer_Id, Country_Code, Customer_Name, DURING)
VALUES (1, 1, 'Mari Tamm', '[2015-02-01, 2015-07-01)');

/*
Test_Chk_When_Unpacked_Then_Key - when these two statements are executed in the given order then the second statement should fail with an error "If at any given time table fte.customer_name_hist contains two distinct rows that are identical except for their Customer_Name and DURING value, then their DURING values il and i2 must be such that il OVERLAPS i2 is false"
*/
BEGIN;
INSERT INTO FTE.CUSTOMER_HIST (Customer_Id, Country_Code, DURING)
VALUES (1, 1, '[2015-01-01, 2015-02-01)');
INSERT INTO FTE.CUSTOMER_SEGMENT_HIST (Customer_Id, Country_Code, Customer_Segment_Code, DURING)
VALUES (1, 1, 1,'[2015-01-01, 2015-02-01)');
INSERT INTO FTE.CUSTOMER_NAME_HIST (Customer_Id, Country_Code, Customer_Name, DURING)
VALUES (1, 1, 'Mari Tamm', '[2015-01-01, 2015-02-01)');
COMMIT;
--an attempt to produce contradiction
INSERT INTO FTE.CUSTOMER_NAME_HIST (Customer_Id, Country_Code, Customer_Name, DURING)
VALUES (1, 1, 'Mari Kask', '[2015-01-30, 2015-07-01)');

/*
Test_Chk_No_Redundancy_Across_Since_And_During - when these two statements are executed in the given order then the second statement should fail with an error "No redundancy is allowed across records in CUSTOMER and CUSTOMER_NAME_HIST"
*/
BEGIN;
INSERT INTO FTE.CUSTOMER_HIST (Customer_Id, Country_Code, DURING)
VALUES (1, 1, '[2015-01-01, 2015-02-01)');
INSERT INTO FTE.CUSTOMER_SEGMENT_HIST (Customer_Id, Country_Code, Customer_Segment_Code, DURING)
VALUES (1, 1, 1,'[2015-01-01, 2015-02-01)');
INSERT INTO FTE.CUSTOMER_NAME_HIST (Customer_Id, Country_Code, Customer_Name, DURING)
VALUES (1, 1, 'Mari Tamm', '[2015-01-01, 2015-02-01)');
COMMIT;
--an attempt to produce redundancy across the history table and the current table
INSERT INTO FTE.CUSTOMER (Customer_Id, Country_Code, Customer_Since, Customer_Name, Customer_Name_Since, Customer_Segment_Code, Customer_Segment_Since)
VALUES (1, 1, '2015-01-01', 'Mari Tamm', '2015-01-31', 1, '2015-01-01');

/*
Test_Chk_No_Redundancy_Across_During_And_Since - when these two statements are executed in the given order then the second statement should fail with an error "No redundancy is allowed across records in CUSTOMER and CUSTOMER_NAME_HIST (CUSTOMER.Customer_Name_Since must be later than CUSTOMER_NAME_HIST.DURATION end point)"
*/
INSERT INTO FTE.CUSTOMER (Customer_Id, Country_Code, Customer_Since, Customer_Name, Customer_Name_Since, Customer_Segment_Code, Customer_Segment_Since)
VALUES (1, 1, '2015-01-01', 'Mari Tamm', '2015-01-01', 1, '2015-01-01');
--an attempt to produce redundancy across the history table and the current table
INSERT INTO FTE.CUSTOMER_NAME_HIST (Customer_Id, Country_Code, Customer_Name, DURING)
VALUES (1, 1, 'Mari Tamm', '[2015-01-01, 2015-02-01)');

/*
Test_Chk_No_Circumlocution_Across_Since_And_During - when these two statements are executed in the given order then the second statement should fail with an error "No circumlocution is allowed across records in CUSTOMER and CUSTOMER_NAME_HIST (CUSTOMER.Customer_Name_Since must be later than CUSTOMER_NAME_HIST.DURATION end point+1 in case of equal attribute values)"
*/
INSERT INTO FTE.CUSTOMER (Customer_Id, Country_Code, Customer_Since, Customer_Name, Customer_Name_Since, Customer_Segment_Code, Customer_Segment_Since)
VALUES (1, 1, '2015-01-01', 'Mari Tamm', '2015-01-01', 1, '2015-01-01');
--an attempt to produce circumlocution across the history table and the current table
INSERT INTO FTE.CUSTOMER_NAME_HIST (Customer_Id, Country_Code, Customer_Name, DURING)
VALUES (1, 1, 'Mari Tamm', '[2014-12-30, 2015-01-01)');

/*
Test_Chk_No_Circumlocution_Across_During_And_Since - when these two statements are executed in the given order then the second statement should fail with an error "No circumlocution is allowed across records in CUSTOMER and CUSTOMER_NAME_HIST (CUSTOMER.Customer_Name_Since must be later than CUSTOMER_NAME_HIST.DURATION end point+1 in case of equal attribute values)"
*/
BEGIN;
INSERT INTO FTE.CUSTOMER_HIST (Customer_Id, Country_Code, DURING)
VALUES (1, 1, '[2015-01-01, 2015-02-02)');
INSERT INTO FTE.CUSTOMER_SEGMENT_HIST (Customer_Id, Country_Code, Customer_Segment_Code, DURING)
VALUES (1, 1, 1,'[2015-01-01, 2015-02-02)');
INSERT INTO FTE.CUSTOMER_NAME_HIST (Customer_Id, Country_Code, Customer_Name, DURING)
VALUES (1, 1, 'Mari Tamm', '[2015-01-01, 2015-02-02)');
COMMIT;
--an attempt to produce circumlocution across the history table and the current table
INSERT INTO FTE.CUSTOMER (Customer_Id, Country_Code, Customer_Since, Customer_Name, Customer_Name_Since, Customer_Segment_Code, Customer_Segment_Since)
VALUES (1, 1, '2015-01-01', 'Mari Tamm', '2015-02-02', 1, '2015-01-01');

/*
Test_Chk_Attribute_Temporal_Integrity1 - when this statement is executed then it should fail with an error "Entity and its attribute have some gaps in their relationship"
*/
INSERT INTO FTE.CUSTOMER (Customer_Id, Country_Code, Customer_Since, Customer_Name, Customer_Name_Since, Customer_Segment_Code, Customer_Segment_Since)
VALUES (1, 1, '2015-01-01', 'Mari Tamm', '2015-01-03', 1, '2015-01-01');
/*
Test_Chk_Attribute_Temporal_Integrity1 - when this statement is executed then it should fail with an error "Entity and its attribute have some gaps in their relationship"
*/
INSERT INTO FTE.CUSTOMER_NAME_HIST (Customer_Id, Country_Code, Customer_Name, DURING)
VALUES (1, 1, 'Mari Kask', DATERANGE '[2015-01-01, 2015-03-01)');
/*
Test_Chk_Attribute_Temporal_Integrity2 - when this statement is executed then it should fail with an error "Entity and its attribute have some gaps in their relationship"
*/
INSERT INTO FTE.CUSTOMER_HIST (Customer_Id, Country_Code, DURING)
VALUES (1, 1, DATERANGE '[2015-01-01, 2015-03-01)');
/* And this should succeed*/
BEGIN;
INSERT INTO FTE.CUSTOMER (Customer_Id, Country_Code, Customer_Since, Customer_Name, Customer_Name_Since, Customer_Segment_Code, Customer_Segment_Since)
VALUES (1, 1, '2015-01-01', 'Mari Tamm', '2015-03-01', 1, '2015-01-01');
INSERT INTO FTE.CUSTOMER_NAME_HIST (Customer_Id, Country_Code, Customer_Name, DURING)
VALUES (1, 1, 'Mari Kask', DATERANGE '[2015-01-01, 2015-03-01)');
COMMIT;

/*
Test_Chk_Since_In_FK_Since - when the latter statement is executed then it should fail with an error "Property cannot exist before SINCE value of referenced table: CUSTOMER"
*/
INSERT INTO FTE.CUSTOMER (Customer_Id, Country_Code, Customer_Since, Customer_Name, Customer_Name_Since, Customer_Segment_Code, Customer_Segment_Since)
VALUES (1, 1, '2015-01-01', 'Mari Tamm', '2015-01-01', 1, '2015-01-01');
INSERT INTO FTE.CUSTOMER_CONTRACT (Customer_Id, Country_Code, Contract_Nbr, Customer_Contract_Since)
VALUES (1, 1, 'A1', '2014-12-01');

/*
Test_Chk_Integrity_In_FK_Tables - when this statement is executed (and CUSTOMER nor CUSTOMER_HIST contain rows about this customer during this period) then it should fail with an error "Entity and its referenced entity have some gaps in their relationship"
*/
INSERT INTO FTE.CUSTOMER_CONTRACT_HIST (Customer_Id, Country_Code, Contract_Nbr, DURING)
VALUES (1, 1, 'A1', '[2014-12-01, 2015-04-01)');
--but this should work:
INSERT INTO FTE.CUSTOMER (Customer_Id, Country_Code, Customer_Since, Customer_Name, Customer_Name_Since, Customer_Segment_Code, Customer_Segment_Since)
VALUES (1, 1, '2014-12-01', 'Mari Tamm', '2014-12-01', 1, '2014-12-01');
INSERT INTO FTE.CUSTOMER_CONTRACT_HIST (Customer_Id, Country_Code, Contract_Nbr, DURING)
VALUES (1, 1, 'A1', '[2014-12-01, 2015-04-01)');
