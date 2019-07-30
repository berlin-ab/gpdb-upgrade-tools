drop schema gpdb_partition_check cascade;
create schema gpdb_partition_check;


create function gpdb_partition_check.find_leaf_partitions() returns table (leaf_table regclass, parent_tuple_oid regclass) as $$
	select parchildrelid, paroid from pg_partition_rule where paroid in (
		select oid from pg_partition outer_pg_partition WHERE parlevel = (
			select max(parlevel) from pg_partition
			where pg_partition.parrelid = outer_pg_partition.parrelid
		)
	)
$$ language sql;


create function gpdb_partition_check.find_leaves_with_root_oid() returns table (parent_oid regclass, leaf_table regclass) as $$
	select pg_partition.parrelid, leaves.leaf_table from gpdb_partition_check.find_leaf_partitions() as leaves
		inner join pg_partition on leaves.parent_tuple_oid = pg_partition.oid
$$ language sql;


create function gpdb_partition_check.find_leaf_policies_with_root_policies() returns table (
	parent_oid regclass, leaf_table regclass, root_localoid regclass, root_attrnums smallint[], leaf_localoid regclass, leaf_attrnums smallint[]
	) as $$
	select * from gpdb_partition_check.find_leaves_with_root_oid() as leaves
		inner join gp_distribution_policy parent_policy on parent_policy.localoid = leaves.parent_oid
		inner join gp_distribution_policy child_policy on child_policy.localoid = leaves.leaf_table
$$ language sql;


create function gpdb_partition_check.distribution_policy_attributes_for_table(table_oid regclass) returns table(attnum smallint) as $$
	select unnest(attrnums)
	    from gp_distribution_policy
	    where localoid = $1;
$$ language sql;


create function gpdb_partition_check.find_distribution_for_table(some_table regclass) returns name[] as $$
	select array_agg(attname order by row_number) attributes
		from pg_attribute join (
			select attnum, row_number() over() from gpdb_partition_check.distribution_policy_attributes_for_table($1)
		) t(attnum, row_number) on pg_attribute.attnum = t.attnum
		where attrelid = $1;
$$ language sql;


create function gpdb_partition_check.get_namespace_oid(schema_name text) returns oid as $$
    select oid
        from pg_namespace
        where pg_namespace.nspname = $1;
$$ language sql;


create function gpdb_partition_check.find_partitions_in_namespace(schema_name text) returns table(
	leaf_table regclass,
	root_table regclass
) as $$
    select parchildrelid, parrelid from pg_partition_rule
        inner join pg_partition on pg_partition_rule.paroid = pg_partition.oid
        where parrelid in (
            select oid from pg_class
            where relnamespace = gpdb_partition_check.get_namespace_oid($1)
        );
$$ language sql;


create function gpdb_partition_check.get_attributes_for_primary_key_constraint_on_table(table_oid regclass) returns table (attnum smallint, row_number bigint) as $$
    select attnum, row_number() over() from (
        select unnest(conkey)
        from pg_constraint
        where conrelid = $1
        and contype = 'p'
    ) constraint_keys(attnum);
$$ language sql;


create function gpdb_partition_check.find_primary_key_constraint_for(table_oid regclass) returns name[] as $$
	select array_agg(attname order by row_number) attributes
		from pg_attribute join gpdb_partition_check.get_attributes_for_primary_key_constraint_on_table($1) t(attnum, row_number)  on pg_attribute.attnum = t.attnum
		where attrelid = $1;
$$ language sql;


--
-- Public interface:
--


--
-- find_leaf_partitions_with_mismatching_policies_to_root:
--
create function gpdb_partition_check.find_leaf_partitions_with_mismatching_policies_to_root(schema_name text) returns table (
	leaf_table regclass,
	root_table regclass
) as $$
	select leaf_table, root_localoid from
		gpdb_partition_check.find_leaf_policies_with_root_policies() as all_leaves
		inner join pg_namespace on pg_namespace.nspname = $1
		inner join pg_class on (pg_class.oid = leaf_table and pg_class.relnamespace = pg_namespace.oid)
		where gpdb_partition_check.find_distribution_for_table(leaf_table)
			IS DISTINCT FROM gpdb_partition_check.find_distribution_for_table(root_localoid)
$$ language sql;


--
-- find_leaf_parititions_with_conflicting_distribution_keys_to_constraints:
--
create function gpdb_partition_check.find_leaf_parititions_with_conflicting_distribution_keys_to_constraints(schema_name text) returns table (
	leaf_table regclass,
	root_table regclass
) as $$
	select leaf_table, root_table from gpdb_partition_check.find_partitions_in_namespace($1)
		inner join pg_constraint on pg_constraint.conrelid = leaf_table and contype = 'p'
		where gpdb_partition_check.find_primary_key_constraint_for(leaf_table)
			!= gpdb_partition_check.find_distribution_for_table(root_table);
$$ language sql;
