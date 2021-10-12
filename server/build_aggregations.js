// Servers
db.servers.aggregate([
    {$match : QUERY },
    {$unwind: "$hardware.disk"},
    {$project: {
        _id: 0,
        "general.server_id": 1,
        "hardware.disk": 1,
        "report_period": 1,
        "ganymede_doc_version": 1}
    },
    {$out: "serverdisksAGGID"}
]);

db.servers.aggregate([
    {$match : QUERY },
    {$unwind: "$hardware.nic"},
    {$project: {
        _id: 0,
        "general.server_id": 1,
        "hardware.nic.nic_id": 1,
        "report_period": 1,
        "ganymede_doc_version": 1}
    },
    {$out: "servernicsAGGID"}
]);

db.servers.aggregate([
    {$match : QUERY },
    {$unwind: "$hardware.software_label"},
    {$project: {
        _id: 0,
        "general.server_id": 1,
        "hardware.software_label.display_name": 1,
        "report_period": 1,
        "ganymede_doc_version": 1}
    },
    {$out: "serversoftwarelabelsAGGID"}
]);

// Images
db.images.aggregate([
    {$match : QUERY },
    {$unwind: "$hardware.disk"},
    {$project: {
        _id: 0,
        "general.image_id": 1,
        "hardware.disk": 1,
        "report_period": 1,
        "ganymede_doc_version": 1}
    },
    {$out: "imagedisksAGGID"}
]);

// MCPs
db.mcps.aggregate([
    {$match : QUERY },
    {$unwind: "$sites"},
    {$project: {
        _id: 0,
        "geo": 1,
        "sites.mcp_id": 1,
        "sites.display_name": 1,
        "sites.type": 1,
        "sites.site_name": 1,
        "report_period": 1,
        "ganymede_doc_version": 1}
    },
    {$out: "geosAGGID"}
]);

// Consistency Groups
db.drs_cluster_pairs.aggregate([
    {$match : QUERY },
    {$unwind: "$general.consistency_groups"},
    {$project: {
        _id: 0,
        "general.drs_pair_id": 1,
        "report_period": 1,
        "ganymede_doc_version": 1,
        "general.consistency_groups.cg_id": 1,
        "general.consistency_groups.state": 1}
    },
    {$out: "consistency_groupsAGGID"}
]);

/*
// For future consideration
// SolidFire Accounts
db.solidfire.aggregate([
    {$match : QUERY },
    {$unwind: "$stats.accounts"},
    {$project: {
        _id: 0,
        "geo": 1,
        "hostname": 1,
        "name": 1,
        "stats.accounts.accountID": 1,
        "stats.accounts.username": 1,
        "report_period": 1,
        "ganymede_doc_version": 1}
    },
    {$out: "sf_accountsAGGID"}
]);

// SolidFire Nodes
db.solidfire.aggregate([
    {$match : QUERY },
    {$unwind: "$stats.nodes"},
    {$project: {
        _id: 0,
        "geo": 1,
        "hostname": 1,
        "name": 1,
        "stats.nodes.nodeID": 1,
        "stats.nodes.nodeType": 1,
        "report_period": 1,
        "ganymede_doc_version": 1}
    },
    {$out: "sf_nodesAGGID"}
]);*/
