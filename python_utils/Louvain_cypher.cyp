:param limit => ( 42);
:param config => ({
  relationshipWeightProperty: null,
  includeIntermediateCommunities: false,
  seedProperty: ''
});
:param communityNodeLimit => ( 10);
:param graphConfig => ({
  nodeProjection: 'User',
  relationshipProjection: {
    relType: {
      type: 'MENTIONED',
      orientation: 'NATURAL',
      properties: {}
    }
  }
});
:param generatedName => ('in-memory-graph-1680684405666');

CALL gds.graph.project($generatedName, $graphConfig.nodeProjection, $graphConfig.relationshipProjection, {})

CALL gds.graph.project($generatedName, $graphConfig.nodeProjection, $graphConfig.relationshipProjection, {})
CALL gds.louvain.stream($generatedName, $config)
YIELD nodeId, communityId AS community, intermediateCommunityIds AS communities
WITH gds.util.asNode(nodeId) AS node, community, communities
WITH community, communities, collect(node) AS nodes
RETURN community, communities, nodes[0..$communityNodeLimit] AS nodes, size(nodes) AS size
ORDER BY size DESC
LIMIT toInteger($limit)

CALL gds.graph.drop($generatedName)