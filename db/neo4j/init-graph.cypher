// Neo4j Cypher Schema & Initial Seed Data for Multimodal Route Planning
// All statements, properties, and comments are written in English.

// 1. Clean existing graph
MATCH (n) DETACH DELETE n;

// 2. Create Constraints for Unique Identification
CREATE CONSTRAINT stop_id_unique IF NOT EXISTS
FOR (s:Stop) REQUIRE s.id IS UNIQUE;

// 3. Create Transit Stations and Stop Nodes
// Stops have names, coordinates (latitude, longitude)
CREATE (s1:Stop:MetroStation {id: 'M-CENTRAL', name: 'Central Metro Station', lat: -12.046374, lon: -77.042793})
CREATE (s2:Stop:MetroStation {id: 'M-NORTH', name: 'North Metro Station', lat: -12.012543, lon: -77.051210})
CREATE (s3:Stop:BusStop {id: 'B-PLAZA', name: 'Plaza Bus Stop', lat: -12.049811, lon: -77.039201})
CREATE (s4:Stop:BusStop {id: 'B-AVENUE', name: 'Central Avenue Bus Stop', lat: -12.032104, lon: -77.045610})
CREATE (s5:Stop:ScooterDock {id: 'S-DOCK1', name: 'Scooter Dock Central Mall', lat: -12.047101, lon: -77.040122})
CREATE (s6:Stop:ScooterDock {id: 'S-DOCK2', name: 'Scooter Dock Office Hub', lat: -12.041021, lon: -77.043511});

// 4. Create Multimodal Transport Segments (Relationships)
// Property parameters:
// - distance_km (Double): Distance in Kilometers
// - time_min (Double): Travel time in minutes
// - cost_usd (Double): Financial cost of segment
// - co2_emissions_g (Double): Grams of CO2 emitted

// METRO Segments (Zero or extremely low CO2)
CREATE (s1)-[:CONNECTS_TO {mode: 'METRO', line: 'Line 1', distance_km: 4.2, time_min: 6.0, cost_usd: 2.00, co2_emissions_g: 0.0}]->(s2)
CREATE (s2)-[:CONNECTS_TO {mode: 'METRO', line: 'Line 1', distance_km: 4.2, time_min: 6.0, cost_usd: 2.00, co2_emissions_g: 0.0}]->(s1)

// BUS Segments (Standard diesel/electric bus emissions)
CREATE (s3)-[:CONNECTS_TO {mode: 'BUS', line: 'Route 405', distance_km: 2.1, time_min: 12.0, cost_usd: 1.50, co2_emissions_g: 178.5}]->(s4)
CREATE (s4)-[:CONNECTS_TO {mode: 'BUS', line: 'Route 405', distance_km: 2.1, time_min: 12.0, cost_usd: 1.50, co2_emissions_g: 178.5}]->(s3)

// SCOOTER Segments (Micromobility, zero operating emissions)
CREATE (s5)-[:CONNECTS_TO {mode: 'SCOOTER', distance_km: 0.8, time_min: 4.5, cost_usd: 1.20, co2_emissions_g: 0.0}]->(s6)
CREATE (s6)-[:CONNECTS_TO {mode: 'SCOOTER', distance_km: 0.8, time_min: 4.5, cost_usd: 1.20, co2_emissions_g: 0.0}]->(s5)

// PEDESTRIAN Transfer / Walk Segments (Connects different modes, Zero Cost, Zero CO2, slower time)
CREATE (s1)-[:CONNECTS_TO {mode: 'WALK', distance_km: 0.3, time_min: 4.0, cost_usd: 0.00, co2_emissions_g: 0.0}]->(s3)
CREATE (s3)-[:CONNECTS_TO {mode: 'WALK', distance_km: 0.3, time_min: 4.0, cost_usd: 0.00, co2_emissions_g: 0.0}]->(s1)

CREATE (s1)-[:CONNECTS_TO {mode: 'WALK', distance_km: 0.2, time_min: 2.5, cost_usd: 0.00, co2_emissions_g: 0.0}]->(s5)
CREATE (s5)-[:CONNECTS_TO {mode: 'WALK', distance_km: 0.2, time_min: 2.5, cost_usd: 0.00, co2_emissions_g: 0.0}]->(s1)

CREATE (s4)-[:CONNECTS_TO {mode: 'WALK', distance_km: 0.4, time_min: 5.0, cost_usd: 0.00, co2_emissions_g: 0.0}]->(s6)
CREATE (s6)-[:CONNECTS_TO {mode: 'WALK', distance_km: 0.4, time_min: 5.0, cost_usd: 0.00, co2_emissions_g: 0.0}]->(s4);

// Index for routing lookups
CREATE INDEX stop_coords_index IF NOT EXISTS
FOR (s:Stop) ON (s.lat, s.lon);
