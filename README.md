# GPDB Upgrade Tools

## Partition checks

**Step 1**

Load the partition check library

```bash
    psql -f partition-check.sql
```

**Step 2**

Enter a psql prompt:

```bash
    psql [your connection parameters]
```

**Step 3** 

Identify tables with known problems:

*note: replace 'some_schema_name' with your schema*

```postgres-psql
    -- search for partition leaf tables that do not match their root's distribution policy
    select * from gpdb_partition_check.find_leaf_partitions_with_mismatching_policies_to_root('some_schema_name');

    -- search for partition leaf tables that have a primary key that conflicts with the distribution key
    select * from gpdb_partition_check.find_leaf_parititions_with_conflicting_distribution_keys_to_constraints('some_schema_name');
```
    
## Developers

- requirements.txt file lives in `test/requirements.txt`
- test script will install dependencies

### Testing

```bash
    export PGPORT=15432

    ./scripts/test
```
