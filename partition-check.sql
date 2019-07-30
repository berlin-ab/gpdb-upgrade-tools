drop function if exists find_leaf_partitions();
drop function if exists find_leaves_with_root_oid();
drop function if exists find_leaf_policies_with_root_policies();
drop function if exists find_leaf_partitions_with_mismatching_policies_to_root(text);
drop function if exists policy(regclass);
drop function if exists find_distribution_for_table(regclass);


create function find_leaf_partitions() returns table (leaf_table regclass, parent_tuple_oid regclass) as $$
	select parchildrelid, paroid from pg_partition_rule where paroid in (
		select oid from pg_partition outer_pg_partition WHERE parlevel = (
			select max(parlevel) from pg_partition
			where pg_partition.parrelid = outer_pg_partition.parrelid
		)
	)
$$ language sql;


create function find_leaves_with_root_oid() returns table (parent_oid regclass, leaf_table regclass) as $$
	select pg_partition.parrelid, leaves.leaf_table from find_leaf_partitions() as leaves
		inner join pg_partition on leaves.parent_tuple_oid = pg_partition.oid
$$ language sql;


create function find_leaf_policies_with_root_policies() returns table (
	parent_oid regclass, leaf_table regclass, root_localoid regclass, root_attrnums smallint[], leaf_localoid regclass, leaf_attrnums smallint[]
	) as $$
	select * from find_leaves_with_root_oid() as leaves
		inner join gp_distribution_policy parent_policy on parent_policy.localoid = leaves.parent_oid
		inner join gp_distribution_policy child_policy on child_policy.localoid = leaves.leaf_table
$$ language sql;


create function policy(table_oid regclass) returns table(attnum smallint) as $$
	select unnest(attrnums) from gp_distribution_policy where localoid = $1;
$$ language sql;


create function find_distribution_for_table(some_table regclass) returns name[] as $$
	select array_agg(attname order by row_number) attributes
		from pg_attribute join (
			select attnum, row_number() over() from policy($1)
		) t(attnum, row_number) on pg_attribute.attnum = t.attnum
		where attrelid = $1;
$$ language sql;


create function find_leaf_partitions_with_mismatching_policies_to_root(schema_name text) returns table(
	leaf_table regclass,
	root_table regclass
) as $$
	select leaf_table, root_localoid from
		find_leaf_policies_with_root_policies() as all_leaves
		inner join pg_namespace on pg_namespace.nspname = $1
		inner join pg_class on (pg_class.oid = leaf_table and pg_class.relnamespace = pg_namespace.oid)
		where find_distribution_for_table(leaf_table)
			IS DISTINCT FROM find_distribution_for_table(root_localoid)
$$ language sql;
