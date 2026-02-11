#!/bin/bash

echo "=== Schema FORWARD Compatibility Testing ==="
echo ""
echo "Current schema: event_id, user_id, event_type, device, event_time, env"
echo "Current mode: FORWARD"
echo ""
echo "FORWARD compatibility means: OLD schema can read NEW data"
echo "New producers can send new data → Old consumers can still read it"
echo ""

# Test 1: FORWARD - Add optional field
echo "TEST 1: FORWARD - Adding optional field 'location' (should PASS)"
echo "Explanation: Old consumer can ignore the new 'location' field"
curl -s -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema":"{\"type\":\"record\",\"name\":\"UserEvent\",\"namespace\":\"com.demo.events\",\"fields\":[{\"name\":\"event_id\",\"type\":\"string\"},{\"name\":\"user_id\",\"type\":\"string\"},{\"name\":\"event_type\",\"type\":\"string\"},{\"name\":\"device\",\"type\":\"string\"},{\"name\":\"event_time\",\"type\":\"long\"},{\"name\":\"env\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"location\",\"type\":[\"null\",\"string\"],\"default\":null}]}"}' \
  http://localhost:8081/compatibility/subjects/user_events-value/versions/latest | jq '.'
echo ""

# Test 2: FORWARD - Add required field
echo "TEST 2: FORWARD - Adding required field 'ip_address' (should PASS)"
echo "Explanation: Old consumer can skip required fields if new producer always sends them"
curl -s -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema":"{\"type\":\"record\",\"name\":\"UserEvent\",\"namespace\":\"com.demo.events\",\"fields\":[{\"name\":\"event_id\",\"type\":\"string\"},{\"name\":\"user_id\",\"type\":\"string\"},{\"name\":\"event_type\",\"type\":\"string\"},{\"name\":\"device\",\"type\":\"string\"},{\"name\":\"event_time\",\"type\":\"long\"},{\"name\":\"env\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"ip_address\",\"type\":\"string\"}]}"}' \
  http://localhost:8081/compatibility/subjects/user_events-value/versions/latest | jq '.'
echo ""

# Test 3: FORWARD - Remove field
echo "TEST 3: FORWARD - Removing field 'device' (should PASS)"
echo "Explanation: Old consumer doesn't need the 'device' field, new schema without it works fine"
curl -s -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema":"{\"type\":\"record\",\"name\":\"UserEvent\",\"namespace\":\"com.demo.events\",\"fields\":[{\"name\":\"event_id\",\"type\":\"string\"},{\"name\":\"user_id\",\"type\":\"string\"},{\"name\":\"event_type\",\"type\":\"string\"},{\"name\":\"event_time\",\"type\":\"long\"},{\"name\":\"env\",\"type\":[\"null\",\"string\"],\"default\":null}]}"}' \
  http://localhost:8081/compatibility/subjects/user_events-value/versions/latest | jq '.'
echo ""

# Test 4: FORWARD - Remove required field (with default)
echo "TEST 4: FORWARD - Removing field with default value (should PASS)"
echo "Explanation: Old consumer can use the default value if field is missing"
curl -s -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema":"{\"type\":\"record\",\"name\":\"UserEvent\",\"namespace\":\"com.demo.events\",\"fields\":[{\"name\":\"event_id\",\"type\":\"string\"},{\"name\":\"user_id\",\"type\":\"string\"},{\"name\":\"event_type\",\"type\":\"string\"},{\"name\":\"device\",\"type\":\"string\"},{\"name\":\"event_time\",\"type\":\"long\"}]}"}' \
  http://localhost:8081/compatibility/subjects/user_events-value/versions/latest | jq '.'
echo ""

echo "=== Completed FORWARD compatibility tests ==="
echo ""
echo "Summary:"
echo "✅ FORWARD allows: Add fields (optional or required), Remove optional fields"
echo "❌ FORWARD doesn't allow: Change field types, Remove required fields without defaults"


Key Findings About FORWARD Compatibility:
# What Works:

# Add optional fields (with or without defaults)
# Remove optional fields
# Remove required fields that have defaults
# What Doesn't Work:

# Add required fields without defaults (TEST 2 failed)
# Change field types
# Make an optional field required
