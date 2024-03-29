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
    select * from gpdb_distribution_check.find_leaf_partitions_with_mismatching_policies_to_root('some_schema_name');

    -- search for tables that have a distribution key that is not a left-subset of its unique constraints
    select * from gpdb_distribution_check.find_tables_conflicting_uniq_const_to_dist_keys('some_schema_name');
```
    
## Developers

- requirements.txt file lives in `test/requirements.txt`
- test script will install dependencies

### Testing

```bash
    export PGPORT=15432

    ./scripts/test
```
