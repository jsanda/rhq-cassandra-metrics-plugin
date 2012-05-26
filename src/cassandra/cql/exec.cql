drop keyspace rhq;
create keyspace rhq 
    with strategy_class = 'SimpleStrategy' and
    strategy_options:replication_factor = 1;

use rhq;

create columnfamily raw_metrics (
    schedue_id int primary key
) with comparator = timestamp and default_validation = double;
