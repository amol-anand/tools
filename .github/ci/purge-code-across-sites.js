// This file is used to purge the code across all sites
try {
  console.log(`env: ${JSON.stringify(process.env.tokens)}`);
  // const payload = JSON.stringify(github.context.payload, undefined, 2)
  // console.log(`The event payload: ${payload}`);
} catch (error) {
  console.error(error.message);
}