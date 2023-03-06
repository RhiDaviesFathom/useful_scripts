export OWNER="fathom-global"
export REPOSITORY="fathom-complex-model"
export WORKFLOW="lint_and_test"

gh api -X GET /repos/$OWNER/$REPOSITORY/actions/runs --paginate \
  | jq '.workflow_runs[] | select(.name == '\"$WORKFLOW\"') | .id' \
  | xargs -t -I{} gh api -X DELETE /repos/$OWNER/$REPOSITORY/actions/runs/{}