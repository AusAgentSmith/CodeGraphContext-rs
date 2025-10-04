# Exit if not by you
if [ "$VERCEL_GIT_COMMIT_AUTHOR_LOGIN" != "Shashankss1205" ]; then 
  echo "Commit not by you, skipping build"
  exit 0
fi

# Exit if no changes in ./website
git fetch --depth=2 origin $VERCEL_GIT_COMMIT_REF
git diff --quiet HEAD^ HEAD -- ./website
if [ $? -eq 0 ]; then
  echo "No changes in ./website, skipping build"
  exit 0
fi

echo "Proceeding with build..."
