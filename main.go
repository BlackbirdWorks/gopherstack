package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"Gopherstack/dynamodb"
)

var db = dynamodb.NewInMemoryDB()

func main() {
	http.HandleFunc("/", handleRequest)
	port := ":8000"
	fmt.Printf("Starting Gopherstack (DynamoDB local) on port %s...\n", port)
	if err := http.ListenAndServe(port, nil); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}

func handleRequest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	target := r.Header.Get("X-Amz-Target")
	parts := strings.Split(target, ".")
	if len(parts) != 2 {
		http.Error(w, "Invalid Target", http.StatusBadRequest)
		return
	}
	action := parts[1]

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read body", http.StatusInternalServerError)
		return
	}
	defer func() {
		_ = r.Body.Close()
	}()

	w.Header().Set("Content-Type", "application/x-amz-json-1.0")

	var response interface{}
	var reqErr error

	switch action {
	case "CreateTable":
		response, reqErr = db.CreateTable(body)
	case "DeleteTable":
		response, reqErr = db.DeleteTable(body)
	case "DescribeTable":
		response, reqErr = db.DescribeTable(body)
	case "ListTables":
		response, reqErr = db.ListTables(body)
	case "PutItem":
		response, reqErr = db.PutItem(body)
	case "GetItem":
		response, reqErr = db.GetItem(body)
	case "DeleteItem":
		response, reqErr = db.DeleteItem(body)
	case "Scan":
		response, reqErr = db.Scan(body)
	case "UpdateItem":
		response, reqErr = db.UpdateItem(body)
	case "Query":
		response, reqErr = db.Query(body)
	case "BatchGetItem":
		response, reqErr = db.BatchGetItem(body)
	case "BatchWriteItem":
		response, reqErr = db.BatchWriteItem(body)
	default:
		fmt.Printf("Unknown action: %s\n", action)
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"__type":"com.amazon.coral.service#UnknownOperationException","message":"Action not supported"}`))
		return
	}

	if reqErr != nil {
		fmt.Printf("Error handling %s: %v\n", action, reqErr)

		if awsErr, ok := reqErr.(*dynamodb.DynamoDBError); ok {
			if strings.Contains(awsErr.Type, "InternalServerError") {
				w.WriteHeader(http.StatusInternalServerError)
			} else {
				w.WriteHeader(http.StatusBadRequest)
			}
			jsonBytes, _ := json.Marshal(awsErr)
			_, _ = w.Write(jsonBytes)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(fmt.Sprintf(`{"__type":"com.amazonaws.dynamodb.v20120810#InternalServerError","message":"%v"}`, reqErr)))
		}
		return
	}

	jsonResponse, _ := json.Marshal(response)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(jsonResponse)
}
