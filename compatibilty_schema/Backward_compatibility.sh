#!/bin/bash

echo "=== Schema Compatibility Testing ==="
echo ""
echo "Current schema: event_id, user_id, event_type, device, event_time, env"
echo "Current mode: BACKWARD"
echo ""

# Test 1: BACKWARD - Add optional field
echo "TEST 1: BACKWARD - Adding optional field 'location' (should PASS)"
curl -s -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema":"{\"type\":\"record\",\"name\":\"UserEvent\",\"namespace\":\"com.demo.events\",\"fields\":[{\"name\":\"event_id\",\"type\":\"string\"},{\"name\":\"user_id\",\"type\":\"string\"},{\"name\":\"event_type\",\"type\":\"string\"},{\"name\":\"device\",\"type\":\"string\"},{\"name\":\"event_time\",\"type\":\"long\"},{\"name\":\"env\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"location\",\"type\":[\"null\",\"string\"],\"default\":null}]}"}' \
  http://localhost:8081/compatibility/subjects/user_events-value/versions/latest | jq '.'
echo ""

# Test 2: BACKWARD - Remove field
echo "TEST 2: BACKWARD - Removing field 'device' (should FAIL)"
curl -s -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema":"{\"type\":\"record\",\"name\":\"UserEvent\",\"namespace\":\"com.demo.events\",\"fields\":[{\"name\":\"event_id\",\"type\":\"string\"},{\"name\":\"user_id\",\"type\":\"string\"},{\"name\":\"event_type\",\"type\":\"string\"},{\"name\":\"event_time\",\"type\":\"long\"},{\"name\":\"env\",\"type\":[\"null\",\"string\"],\"default\":null}]}"}' \
  http://localhost:8081/compatibility/subjects/user_events-value/versions/latest | jq '.'
echo ""

# Test 3: BACKWARD - Add required field
echo "TEST 3: BACKWARD - Adding required field 'ip_address' (should FAIL)"
curl -s -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data '{"schema":"{\"type\":\"record\",\"name\":\"UserEvent\",\"namespace\":\"com.demo.events\",\"fields\":[{\"name\":\"event_id\",\"type\":\"string\"},{\"name\":\"user_id\",\"type\":\"string\"},{\"name\":\"event_type\",\"type\":\"string\"},{\"name\":\"device\",\"type\":\"string\"},{\"name\":\"event_time\",\"type\":\"long\"},{\"name\":\"env\",\"type\":[\"null\",\"string\"],\"default\":null},{\"name\":\"ip_address\",\"type\":\"string\"}]}"}' \
  http://localhost:8081/compatibility/subjects/user_events-value/versions/latest | jq '.'
echo ""

echo "=== Completed BACKWARD compatibility tests ==="


# BACKWARD Compatibility = Good for Adding New Fields
# When you use BACKWARD compatibility mode:

# Adding NEW optional fields with defaults = GOOD ✓

# Old code still works
# New code can use the new field
# No breaking changes

# Removing fields or adding required fields = BAD ✗

# Old code breaks
# Can't handle the changes
