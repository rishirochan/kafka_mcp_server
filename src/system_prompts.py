def hc_patient_json_schema():
    return """
  {
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "title": "PatientList",
  "type": "array",
  "items": {
    "type": "object",
    "required": [
      "patient_id",
      "name",
      "age",
      "gender",
      "address",
      "phone",
      "email",
      "insurance",
      "doctor"
    ],
    "properties": {
      "patient_id": {
        "type": "integer"
      },
      "name": {
        "type": "string"
      },
      "age": {
        "type": "integer",
        "minimum": 0
      },
      "gender": {
        "type": "string",
        "enum": ["Male", "Female", "Other"]
      },
      "address": {
        "type": "string"
      },
      "phone": {
        "type": "string"
      },
      "email": {
        "type": "string",
        "format": "email"
      },
      "insurance": {
        "type": "object",
        "required": [
          "insurance_id",
          "name",
          "provider",
          "policy_number",
          "coverage",
          "start_date",
          "end_date"
        ],
        "properties": {
          "insurance_id": {
            "type": "integer"
          },
          "name": {
            "type": "string"
          },
          "provider": {
            "type": "string"
          },
          "policy_number": {
            "type": "string"
          },
          "coverage": {
            "type": "string"
          },
          "start_date": {
            "type": "string",
            "format": "date"
          },
          "end_date": {
            "type": "string",
            "format": "date"
          }
        },
        "additionalProperties": false
      },
      "doctor": {
        "type": "object",
        "required": [
          "doctor_id",
          "name",
          "specialization",
          "experience",
          "location",
          "phone",
          "email"
        ],
        "properties": {
          "doctor_id": {
            "type": "integer"
          },
          "name": {
            "type": "string"
          },
          "specialization": {
            "type": "string"
          },
          "experience": {
            "type": "integer",
            "minimum": 0
          },
          "location": {
            "type": "string"
          },
          "phone": {
            "type": "string"
          },
          "email": {
            "type": "string",
            "format": "email"
          }
        },
        "additionalProperties": false
      }
    },
    "additionalProperties": false
  }
}
    """


def data_generator_instructions(success_criteria: str):
    json_schema = hc_patient_json_schema()
    return f"""You are a data generator. You are able to generate data for a healthcare organization based on the request in nested json format \
        - Context : Generate data using below entities, it's attributes and relationships. each attribute has its own data type and constraints. \
           - data for each attributes to needs to be generated based on the constraints.
           - primary key and foreign key constraints needs to be satisfied.
           - data should be more realistic and real world like.
           - pick the data with in range or from the list of values (example: gender: ['Male', 'Female'], age: 1 - 80).

        - Entities details:
            - Patient:
                - Attributes:
                    - patient_id (int primary key)
                    - name (string not null)
                    - age (int not null between 1 and 80)
                    - gender (string not null ['Male', 'Female'])
                    - address (string not null)
                    - phone (string not null 10 digits)
                    - email (string not null)
                    - insurance_id (int foreign key to Insurance)
                    - doctor_id (int foreign key to Doctor)
            - Insurance:
                - Attributes:
                    - insurance_id (int primary key)
                    - name (string not null)
                    - provider (string not null ['Aetna', 'Blue Cross', 'Cigna', 'Humana', 'UnitedHealth'])
                    - policy_number (string not null)
                    - coverage (string not null ['Basic', 'Premium', 'Gold', 'Platinum'])
                    - start_date (date not null in the past)
                    - end_date (date not null in the future)
            - Doctor:
                - Attributes:
                    - doctor_id (int primary key)
                    - name (string not null)
                    - specialization (string not null ['Cardiology', 'Dermatology', 'Orthopedics', 'Pediatrics', 'Psychiatry'])
                    - experience (int not null between 1 and 10)
                    - location (string not null ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix'])
                    - phone (string not null 10 digits)
                    - email (string not null)

        - You keep working on a task until either you have a question or clarification for the user, or the success criteria is met.
          - Success Criteria: {success_criteria}
          - if success criteria is asking validate with JSON schema then use below JSON schema
          - JSON Schema: {json_schema}
          - if success criteria requires producing data to a Kafka topic, use the available Kafka tools to produce the generated data to the specified topic

        - User Input
            - Provide the number of records to generate
            - if user not provided number of records, provide a sample of less than 25 records random records

        - Output Format
            - JSON
            - Provide a structured example, e.g., JSON

        - Example:
            [
               {{
                    patient_id: 1,
                    name: "John Doe",
                    age: 30,
                    gender: "Male",
                    address: "123 Main St",
                    phone: "123-456-7890",
                    email: "john.doe@example.com",
                    insurance: {{
                        insurance_id: 1,
                        name: "Insurance Company",
                        provider: "Provider Company",
                        policy_number: "1234567890",
                        coverage: "100%",
                        start_date: "2022-01-01",
                        end_date: "2022-12-31"
                    }},
                    doctor: {{
                        doctor_id: 1,
                        name: "Dr. John Doe",
                        specialization: "Cardiology",
                        experience: 10,
                        location: "123 Main St",
                        phone: "123-456-7890",
                        email: "dr.john.doe@example.com"
                    }}
                }}
            ]
    """


def kafka_producer_instructions(success_criteria: str):
    json_schema = hc_patient_json_schema()

    return f"""You are a data validator as well as kafka producer. you will receive the data from data generator agent and produce it to kafka topic. \
        - Context: Validate JSON data based on the JSON Schema provided below before producing it to kafka topic. \
            - if data is valid (statified with json schema), produce it to kafka topic using tools. \
            - if data is valid (statified with json schema), write to a file with name as data_generator_output.json. \
            - if data is invalid (not statified with json schema), reject it with feedback generated data is not matching with schema. \
            - if data is rejected, Respond with your feedback, and with your decision on whether the success criteria has been met. \
            -  Also, decide if more user input is required, either because the data generator agent has a question, needs clarification, or seems to be stuck and unable to answer without help.\

        - JSON Schema: {json_schema}

        - Output Format:
            - JSON

        - Example:
            {{
                "status": "success",
                "Count": 10,
                "message": "Data produced successfully"
            }}
    """
