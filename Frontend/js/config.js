password = "insert-your-passsword";
user = "neo4j";

const driver = neo4j.driver(
  "bolt://localhost:7687",
  neo4j.auth.basic(user, password)
);

const graphConfig = {
  containerId: "viz",
  neo4j: {
    serverUrl: "bolt://localhost:7687",
    serverUser: user,
    serverPassword: password,
  },
  visConfig: {
    nodes: {
      shape: "dot",
      scaling: {
        label: true,
      },
    },
    edges: {
      arrows: {
        to: { enabled: true },
      },
    },
    physics: {
      hierarchicalRepulsion: {
        avoidOverlap: 1,
      },
      solver: "repulsion",
      repulsion: {
        nodeDistance: 100,
      },
    },
    layout: {
      improvedLayout: true,
      randomSeed: 420,
    },
  },
  labels: {
    User: {
      [NeoVis.NEOVIS_ADVANCED_CONFIG]: {
        function: {
          title: (node) => {
            return node.properties.userId;
          },
          label: (node) => {
            return node.properties.userId.toString();
          },
        },
        static: {
          color: "#87ceeb",
        },
      },
    },
    Movie: {
      label: "title",
      [NeoVis.NEOVIS_ADVANCED_CONFIG]: {
        function: {
          title: (node) => {
            return node.properties.title;
          },
        },
        static: {
          color: "#ff7f50",
        },
      },
    },

    Genre: {
      label: "name",
      [NeoVis.NEOVIS_ADVANCED_CONFIG]: {
        static: {
          color: "#90ee90",
        },
      },
    },
  },

  relationships: {
    RATED: {
      value: "rating",
      [NeoVis.NEOVIS_ADVANCED_CONFIG]: {
        function: {
          title: (relationship) => {
            return relationship.properties.rating;
          },
        },
        static: {
          scaling: { max: 7 },
          color: "#444",
        },
      },
    },
    RECOMMENDED: {
      value: "rating",
      [NeoVis.NEOVIS_ADVANCED_CONFIG]: {
        function: {
          title: (relationship) => {
            return relationship.properties.rating;
          },
        },
        static: {
          scaling: { max: 7 },
          color: "#444",
        },
      },
    },
    BELONGS_TO: {
      [NeoVis.NEOVIS_ADVANCED_CONFIG]: {
        static: {
          color: "#444",
        },
      },
    },
  },
};
