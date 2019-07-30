import unittest
import sqlalchemy
import os


class PartitionCheckTest(unittest.TestCase):
    def setup_database_connection(self):
        port = os.getenv("PGPORT")

        db_string = "postgresql://localhost:{port}/postgres".format(
            port=port,
        )

        self.engine = sqlalchemy.create_engine(db_string)
        self.db = self.engine.connect()

    def create_self_contained_schema(self, schema_name):
        self.db.execute("""
            drop schema if exists {schema_name} cascade;
            create schema {schema_name};
        """.format(schema_name=schema_name))

    def use_schema(self, schema_name):
        self.db.execute("""
            set search_path to {schema_name};
        """.format(schema_name=schema_name))

    def setUp(self):
        self.setup_database_connection()

    def tearDown(self):
        self.db.close()

    def create_table_with_multilevel_partition(self):
        self.db.execute("""
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

    def import_partition_check_library(self):
        script_dir = os.path.dirname(__file__)
        library_path = os.path.join(script_dir, "..", "partition-check.sql")
        with open(library_path) as file:
            self.db.execute(file.read())

    def alter_leaf_node_to_have_a_different_policy(self):
        self.db.execute("""
            alter table example_table_1_prt_ein_2_prt_partition_a_3_prt_partition_c set distributed randomly;
        """)

    def exchange_a_valid_table_into_the_partition_hierarchy_with_alternate_attribute_ordering(self):
        self.db.execute("""
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
        self.create_self_contained_schema('myschema')
        self.use_schema('myschema')
        self.import_partition_check_library()

        self.create_table_with_multilevel_partition()
        self.alter_leaf_node_to_have_a_different_policy()

        rows = self.db.execute("""
            select * from find_leaf_partitions_with_mismatching_policies_to_root('myschema');
        """).fetchall()

        self.assertEqual(len(rows), 1)

        mismatching_result = rows[0]
        self.assertEqual(mismatching_result['leaf_table'], 'example_table_1_prt_ein_2_prt_partition_a_3_prt_partition_c')
        self.assertEqual(mismatching_result['root_table'], 'example_table')

    def test_no_mismatching_partitions_are_returned_for_a_different_schema(self):
        self.create_self_contained_schema('myschema')
        self.create_self_contained_schema('myotherschema')
        self.use_schema('myotherschema')
        self.import_partition_check_library()

        self.create_table_with_multilevel_partition()
        self.alter_leaf_node_to_have_a_different_policy()

        rows = self.db.execute("""
            select * from find_leaf_partitions_with_mismatching_policies_to_root('myschema');
        """).fetchall()

        self.assertEqual(len(rows), 0)

    def test_partitions_with_alternate_attribute_indexing_of_distribution_keys_is_not_considered_mismatching(self):
        self.create_self_contained_schema('myschema')
        self.use_schema('myschema')
        self.import_partition_check_library()

        self.create_table_with_multilevel_partition()
        self.exchange_a_valid_table_into_the_partition_hierarchy_with_alternate_attribute_ordering()

        rows = self.db.execute("""
            select * from find_leaf_partitions_with_mismatching_policies_to_root('myschema');
        """).fetchall()

        self.assertEqual(rows, [])
