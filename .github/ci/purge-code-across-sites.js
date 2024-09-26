// This file is used to purge the code across all sites
try {
  // console.log(`env: ${process.env.ALL_CHANGED_FILES}`);
  const changedFiles = process.env.ALL_CHANGED_FILES.split(' ');
  console.log(`changedFiles: ${changedFiles}`);
} catch (error) {
  console.error(error.message);
}