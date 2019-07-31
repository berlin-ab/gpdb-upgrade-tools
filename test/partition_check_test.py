import unittest
import sqlalchemy
import os


class TestHelpers():
    def __init__(self):
        port = os.getenv("PGPORT")

        db_string = "postgresql://localhost:{port}/postgres".format(
            port=port,
        )

        self.engine = sqlalchemy.create_engine(db_string)
        self.db = self.engine.connect()
        self.db.execute("reset search_path")
        self.schemas = []

    def tearDown(self):
        for schema_name in self.schemas:
            self.drop_schema(schema_name)

        self.db.close()

    def execute(self, query):
        return self.db.execute(query)

    def drop_schema(self, schema_name):
        self.execute("""
            drop schema if exists {schema_name} cascade;
        """.format(schema_name=schema_name))

    def create_schema(self, schema_name):
        self.schemas.append(schema_name)

        self.drop_schema(schema_name)

        self.execute("""
            create schema {schema_name};
        """.format(schema_name=schema_name))

    def use_schema(self, schema_name):
        self.using_schema = schema_name

        self.execute("""
            set search_path to {schema_name};
        """.format(schema_name=schema_name))

    def import_library(self):
        script_dir = os.path.dirname(__file__)
        library_path = os.path.join(script_dir, "..", "partition-check.sql")
        with open(library_path) as file:
            self.execute(file.read())


class MismatchingLeafNodePartitionCheckTest(unittest.TestCase):
    def setUp(self):
        self.test_helpers = TestHelpers()

    def tearDown(self):
        self.test_helpers.tearDown()

    def create_schema(self, schema_name):
        self.test_helpers.create_schema(schema_name)

    def use_schema(self, schema_name):
        self.test_helpers.use_schema(schema_name)

    def create_table_with_multilevel_partition(self):
        self.test_helpers.execute("""
            drop table if exists example_table;
            create table example_table (
                b int,
                a int,
                c int
            )
            distributed by (a)
            partition by range (a)
            subpartition by range (b)
            subpartition by range (c)
            (
                partition ein start (0) end (10) ( -- a
                    subpartition partition_a start (0) end(5) ( -- b
                        subpartition partition_c start (0) end (3), -- c
                        subpartition partition_d start (3) end (5) -- c
                    ),
                    subpartition partition_b start (5) end(10) ( -- b
                        subpartition partition_e start(5) end (10) -- c
                    )
                ),
                partition zwei start (10) end (20) ( -- a
                    subpartition partition_z start (10) end (20) ( -- b
                        subpartition partition_y start (0) end (3) -- c
                    )
                )
            );
        """)

    def import_library(self):
        self.test_helpers.import_library()

    def alter_leaf_node_to_have_a_different_policy(self):
        self.test_helpers.execute("""
            alter table example_table_1_prt_ein_2_prt_partition_a_3_prt_partition_c set distributed randomly;
        """)

    def exchange_a_valid_table_into_the_partition_hierarchy_with_alternate_attribute_ordering(self):
        self.test_helpers.execute("""
            drop table if exists table_to_exchange;
            
            create table table_to_exchange (
                x int,
                b int,
                a int,
                c int
            ) distributed by (a);
            
            alter table table_to_exchange drop column x;
            
            alter table example_table
                alter partition ein
                alter partition partition_a
                exchange partition partition_c
                with table table_to_exchange; 
        """)

    def test_partitions_with_mismatching_policies_are_returned_for_the_given_schema(self):
        self.create_schema('myschema')
        self.use_schema('myschema')
        self.import_library()

        self.create_table_with_multilevel_partition()
        self.alter_leaf_node_to_have_a_different_policy()

        rows = self.test_helpers.execute("""
            select * from gpdb_partition_check.find_leaf_partitions_with_mismatching_policies_to_root('myschema');
        """).fetchall()

        self.assertEqual(len(rows), 1)

        mismatching_result = rows[0]
        self.assertEqual(mismatching_result['leaf_table'], 'example_table_1_prt_ein_2_prt_partition_a_3_prt_partition_c')
        self.assertEqual(mismatching_result['root_table'], 'example_table')

    def test_no_mismatching_partitions_are_returned_for_a_different_schema(self):
        self.create_schema('myschema')
        self.create_schema('myotherschema')
        self.use_schema('myotherschema')
        self.import_library()

        self.create_table_with_multilevel_partition()
        self.alter_leaf_node_to_have_a_different_policy()

        rows = self.test_helpers.execute("""
            select * from gpdb_partition_check.find_leaf_partitions_with_mismatching_policies_to_root('myschema');
        """).fetchall()

        self.assertEqual(len(rows), 0)

    def test_partitions_with_alternate_attribute_indexing_of_distribution_keys_is_not_considered_mismatching(self):
        self.create_schema('myschema')
        self.use_schema('myschema')
        self.import_library()

        self.create_table_with_multilevel_partition()
        self.exchange_a_valid_table_into_the_partition_hierarchy_with_alternate_attribute_ordering()

        rows = self.test_helpers.execute("""
            select * from gpdb_partition_check.find_leaf_partitions_with_mismatching_policies_to_root('myschema');
        """).fetchall()

        self.assertEqual(rows, [])


class ConflictingDistributionKeyWithUniqueIndex(unittest.TestCase):
    def setUp(self):
        self.test_helpers = TestHelpers()

    def tearDown(self):
        self.test_helpers.tearDown()

    def test_returns_tables_that_have_primary_keys_that_are_ordered_differently_than_distribution_key(self):
        self.test_helpers.create_schema('myschema')
        self.test_helpers.use_schema('myschema')
        self.test_helpers.import_library()

        self.test_helpers.execute("""
            drop table if exists example_table;
            
            create table example_table (a int, b int)
                distributed by (a, b)
                partition by range(a) (start (1) end (2) every (1));
                
            alter table example_table add constraint example_table_pkey primary key (b, a);
        """)

        rows = self.test_helpers.execute("""
            select leaf_table, root_table 
                from gpdb_partition_check.find_conflicting_leaf_partitions('myschema')
        """).fetchall()


        self.assertEqual(
            [('example_table_1_prt_1', 'example_table')],
            rows
        )

    def test_returns_any_tables_that_have_primary_keys_that_are_ordered_differently_than_distribution_key(self):
        self.test_helpers.create_schema('myschema')
        self.test_helpers.use_schema('myschema')
        self.test_helpers.import_library()

        self.test_helpers.execute("""
            drop table if exists some_other_example_table;
            
            create table some_other_example_table (a int, b int)
                distributed by (a, b)
                partition by range(a) (start (1) end (2) every (1));
                
            alter table some_other_example_table add constraint some_other_example_table_pkey primary key (b, a);
        """)

        rows = self.test_helpers.execute("""
            select leaf_table, root_table 
                from gpdb_partition_check.find_conflicting_leaf_partitions('myschema')
        """).fetchall()


        self.assertEqual(
            [('some_other_example_table_1_prt_1', 'some_other_example_table')],
            rows
        )

    def test_it_does_not_return_tables_that_are_not_in_the_given_schema(self):
        self.test_helpers.create_schema('myschema')
        self.test_helpers.create_schema('myotherschema')
        self.test_helpers.use_schema('myotherschema')
        self.test_helpers.import_library()

        self.test_helpers.execute("""
            drop table if exists example_table;
            
            create table example_table (a int, b int)
                distributed by (a, b)
                partition by range(a) (start (1) end (2) every (1));
                
            alter table example_table add constraint example_table_pkey primary key (b, a);
        """)

        rows = self.test_helpers.execute("""
            select leaf_table, root_table 
                from gpdb_partition_check.find_conflicting_leaf_partitions('myschema')
        """).fetchall()

        self.assertEqual(rows, [])

    def test_it_does_not_return_tables_that_do_not_conflict(self):
        self.test_helpers.create_schema('myschema')
        self.test_helpers.use_schema('myschema')
        self.test_helpers.import_library()

        self.test_helpers.execute("""
            drop table if exists example_table;
            
            create table example_table (a int, b int)
                distributed by (a, b)
                partition by range(a) (start (1) end (2) every (1));
        """)

        rows = self.test_helpers.execute("""
            select leaf_table, root_table 
                from gpdb_partition_check.find_conflicting_leaf_partitions('myschema')
        """).fetchall()

        self.assertEqual([], rows)

    def test_it_does_not_return_tables_that_do_not_conflict_with_primary_keys(self):
        self.test_helpers.create_schema('myschema')
        self.test_helpers.use_schema('myschema')
        self.test_helpers.import_library()

        self.test_helpers.execute("""
            drop table if exists example_table;
            
            create table example_table (a int, b int)
                distributed by (a, b)
                partition by range(a) (start (1) end (2) every (1));

            alter table example_table add constraint example_table_pkey primary key (a, b);
        """)

        rows = self.test_helpers.execute("""
            select leaf_table, root_table 
                from gpdb_partition_check.find_conflicting_leaf_partitions('myschema')
        """).fetchall()

        self.assertEqual([], rows)

    def test_returns_tables_that_have_unique_indexes_ordered_differently_than_distribution_key(self):
        self.test_helpers.create_schema('myschema')
        self.test_helpers.use_schema('myschema')
        self.test_helpers.import_library()

        self.test_helpers.execute("""
            drop table if exists example_table;

            create table example_table (a int, b int)
                distributed by (a, b)
                partition by range(a) (start (1) end (2) every (1));

            alter table example_table add constraint example_table_uniq unique (b, a);
        """)

        rows = self.test_helpers.execute("""
            select leaf_table, root_table
                from gpdb_partition_check.find_conflicting_leaf_partitions('myschema')
        """).fetchall()

        self.assertEqual(
            [('example_table_1_prt_1', 'example_table')],
            rows
        )

    def test_returns_tables_that_have_a_distribution_key_that_is_not_a_left_subset_of_a_unique_index(self):
        self.test_helpers.create_schema('myschema')
        self.test_helpers.use_schema('myschema')
        self.test_helpers.import_library()

        self.test_helpers.execute("""
            drop table if exists example_table;

            create table example_table (a int, b int, c int)
                distributed by (a, b)
                partition by range(a) (start (1) end (2) every (1));

            alter table example_table add constraint example_table_uniq primary key (a, b);
            alter table example_table set with (reorganize=true) distributed by (b);
        """)

        rows = self.test_helpers.execute("""
            select leaf_table, root_table
                from gpdb_partition_check.find_conflicting_leaf_partitions('myschema')
        """).fetchall()

        self.assertEqual(
            [('example_table_1_prt_1', 'example_table')],
            rows
        )

    def test_does_not_return_tables_that_have_a_distribution_key_that_is_a_left_subset_of_a_unique_index(self):
        self.test_helpers.create_schema('myschema')
        self.test_helpers.use_schema('myschema')
        self.test_helpers.import_library()

        self.test_helpers.execute("""
            drop table if exists example_table;

            create table example_table (a int, b int, c int)
                distributed by (a, b)
                partition by range(a) (start (1) end (2) every (1));

            alter table example_table add constraint example_table_uniq unique (a, b);
            alter table example_table set with (reorganize=true) distributed by (a);
        """)

        rows = self.test_helpers.execute("""
            select leaf_table, root_table
                from gpdb_partition_check.find_conflicting_leaf_partitions('myschema')
        """).fetchall()

        self.assertEqual(
            [],
            rows
        )

    def test_it_returns_conflicts_when_there_are_multiple_unique_constraints(self):
        self.test_helpers.create_schema('myschema')
        self.test_helpers.use_schema('myschema')
        self.test_helpers.import_library()

        self.test_helpers.execute("""
            drop table if exists example_table;

            create table example_table (a int, b int, c int)
                distributed by (a, b)
                partition by range(a) (start (1) end (2) every (1));

            alter table example_table add constraint example_table_uniq unique (a, b);
            alter table example_table add constraint example_table_pkey primary key (b, a);
        """)

        rows = self.test_helpers.execute("""
            select leaf_table, root_table
                from gpdb_partition_check.find_conflicting_leaf_partitions('myschema')
        """).fetchall()

        self.assertEqual(
            [('example_table_1_prt_1', 'example_table')],
            rows
        )
