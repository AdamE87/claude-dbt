# CLAUDE.md
This is my Claude configuration file for this project

## Common Commands
- dbt build: run and test models in the project
- dbt test: run only tests within the project
- dbt compile: compile models within the project

## Project Standards
- All models must have a passing unique key test on them
- All columns defined in unique key tests must pass a not_null test
- All columns in all models must be documentated in a yml file within their relative folder direction.