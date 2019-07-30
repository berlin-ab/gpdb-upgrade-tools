# GPDB Upgrade Tools

## Partition checks

    # load the partition check library
    psql -f partition-check.sql

    # search for partition leaf nodes that do not match their root's distribution policy
    psql -c "select * from find_leaf_partitions_with_mismatching_policies_to_root('some_schema_name');"
    
## Developers

- requirements.txt file lives in `test/requirements.txt`
- test script will install dependencies

### Testing

```bash
    export PGPORT=15432

    ./scripts/test
```
