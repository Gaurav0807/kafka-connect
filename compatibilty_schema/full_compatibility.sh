#!/bin/bash

echo "=== Schema TRANSITIVE Compatibility Testing ==="
echo ""
echo "Current schema: event_id, user_id, event_type, device, event_time, env"
echo "Current mode: TRANSITIVE"
echo ""
echo "TRANSITIVE compatibility means: NEW schema is compatible with ALL previous versions"
echo "Example: v1 → v2 → v3 (v3 must be compatible with both v2 AND v1)"
echo "Most STRICT mode - ensures evolution chain stability"
echo ""

# Test 1: TRANSITIVE - Add optional field
echo "TEST 1: TRANSITIVE - Adding optional field 'location' (should PASS)"
echo "Explanation: Optional fields don't break compatibility with any version"
curl -s -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema":"{\"type\":\"record\",\"name\":\"UserEvent\",\"namespace\":\"com.demo.events\",\"fields\":[{\"name\":\"event_id\",\"type\":\"string\"},{\"name\":\"user_id\",\"type\":\"string\"},{\"name\":\"event_type\",\"type\":\"string\"},{\"name\":\"device\",\"type\":\"string\"},{\"name\":\"event_time\",\"type\":\"long\"},{\"name\":\"env\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"location\",\"type\":[\"null\",\"string\"],\"default\":null}]}"}' \
  http://localhost:8081/compatibility/subjects/user_events-value/versions/latest | jq '.'
echo ""

# Test 2: TRANSITIVE - Add required field
echo "TEST 2: TRANSITIVE - Adding required field 'ip_address' (should FAIL)"
echo "Explanation: Required field breaks compatibility with older versions"
curl -s -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema":"{\"type\":\"record\",\"name\":\"UserEvent\",\"namespace\":\"com.demo.events\",\"fields\":[{\"name\":\"event_id\",\"type\":\"string\"},{\"name\":\"user_id\",\"type\":\"string\"},{\"name\":\"event_type\",\"type\":\"string\"},{\"name\":\"device\",\"type\":\"string\"},{\"name\":\"event_time\",\"type\":\"long\"},{\"name\":\"env\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"ip_address\",\"type\":\"string\"}]}"}' \
  http://localhost:8081/compatibility/subjects/user_events-value/versions/latest | jq '.'
echo ""

# Test 3: TRANSITIVE - Remove field
echo "TEST 3: TRANSITIVE - Removing field 'device' (should FAIL)"
echo "Explanation: Removing field breaks compatibility with older versions that expect it"
curl -s -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema":"{\"type\":\"record\",\"name\":\"UserEvent\",\"namespace\":\"com.demo.events\",\"fields\":[{\"name\":\"event_id\",\"type\":\"string\"},{\"name\":\"user_id\",\"type\":\"string\"},{\"name\":\"event_type\",\"type\":\"string\"},{\"name\":\"event_time\",\"type\":\"long\"},{\"name\":\"env\",\"type\":[\"null\",\"string\"],\"default\":null}]}"}' \
  http://localhost:8081/compatibility/subjects/user_events-value/versions/latest | jq '.'
echo ""

# Test 4: TRANSITIVE - Remove optional field with default
echo "TEST 4: TRANSITIVE - Removing optional field 'env' with default (should PASS)"
echo "Explanation: Safe to remove optional fields with defaults across all versions"
curl -s -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema":"{\"type\":\"record\",\"name\":\"UserEvent\",\"namespace\":\"com.demo.events\",\"fields\":[{\"name\":\"event_id\",\"type\":\"string\"},{\"name\":\"user_id\",\"type\":\"string\"},{\"name\":\"event_type\",\"type\":\"string\"},{\"name\":\"device\",\"type\":\"string\"},{\"name\":\"event_time\",\"type\":\"long\"}]}"}' \
  http://localhost:8081/compatibility/subjects/user_events-value/versions/latest | jq '.'
echo ""

# Test 5: TRANSITIVE - Add multiple optional fields
echo "TEST 5: TRANSITIVE - Adding multiple optional fields (should PASS)"
echo "Explanation: Multiple optional additions are transitive-safe"
curl -s -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema":"{\"type\":\"record\",\"name\":\"UserEvent\",\"namespace\":\"com.demo.events\",\"fields\":[{\"name\":\"event_id\",\"type\":\"string\"},{\"name\":\"user_id\",\"type\":\"string\"},{\"name\":\"event_type\",\"type\":\"string\"},{\"name\":\"device\",\"type\":\"string\"},{\"name\":\"event_time\",\"type\":\"long\"},{\"name\":\"env\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"location\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"region\",\"type\":[\"null\",\"string\"],\"default\":\"US\"}]}"}' \
  http://localhost:8081/compatibility/subjects/user_events-value/versions/latest | jq '.'
echo ""

echo "=== Completed TRANSITIVE compatibility tests ==="
echo ""
echo "Summary:"
echo "TRANSITIVE allows: Add optional fields with defaults, Remove optional fields with defaults"
echo "TRANSITIVE doesn't allow: Add required fields, Remove required fields, Change field types"
echo ""
echo "Why TRANSITIVE matters:"
echo "────────────────────────"
echo "v1 schema: [id, name, email]"
echo "   (add optional 'phone' with default)"
echo "v2 schema: [id, name, email, phone]"
echo "   (add optional 'address' with default)"
echo "v3 schema: [id, name, email, phone, address]"
echo ""
echo "TRANSITIVE ensures v3 is compatible with v1 and v2 (both directions)"
echo "This is CRITICAL for long-running systems with many schema versions"
echo ""
echo "All Modes Ranked by Strictness:"
echo "────────────────────────────────"
echo "1. TRANSITIVE (Most strict) - Compatible with ALL previous versions"
echo "2. FULL                    - Compatible with immediate previous version (both ways)"
echo "3. BACKWARD                - Compatible with immediate previous version (consumers read old data)"
echo "4. FORWARD                 - Compatible with immediate previous version (producers send to old consumers)"

