# Assignment

## Brief

Write the YML for the following questions.

## Instructions

Paste the answer as YML in the answer code section below each question.

### Question 1

Question: Add 2 more tests each using `dbt_utils` and `dbt-expectations` to `fact_sales`.

Answer:

Specs for `dbt_utils`:

```yml
      - name: date
        tests:
        - dbt_utils.accepted_range:
            min_value: "PARSE_DATE('%F', '2012-01-01')"
            max_value: "CURRENT_DATE()"
```

Specs for `dbt-expectations`:

```yml
      - name: date
        tests:
          - dbt_expectations.expect_column_values_to_be_of_type:
              column_type: date
```

## Submission

- Submit the URL of the GitHub Repository that contains your work to NTU black board.
- Should you reference the work of your classmate(s) or online resources, give them credit by adding either the name of your classmate or URL.
