function addErrorMessage(message, el) {
  const errorDiv = document.createElement('div');
  errorDiv.classList.add('log');
  errorDiv.classList.add('error');
  errorDiv.innerHTML = message;
  el.insertAdjacentElement('afterend', errorDiv);
}

async function getConfig() {
  const org = document.getElementById('org').value;
  const site = document.getElementById('site').value;
  if (org && site) {
    const resp = await fetch(`https://admin.hlx.page/config/${org}/sites/${site}.json`, {
      credentials: 'include',
    });
    if (resp.status === 401) {
      const repoSubmit = document.getElementById('repoSubmit');
      addErrorMessage(`401 Unauthorized. Please login to <a href="https://admin.hlx.page/login">https://admin.hlx.page/login</a> before viewing configurations.
        You also need to have a role of admin to view site configurations`, repoSubmit);
    }
    if (resp && resp.ok) {
      const configJson = await resp.json();
      if (configJson) {
        const configDiv = document.querySelector('div.config');
        if (configDiv.classList.contains('hidden')) configDiv.classList.remove('hidden');
        const pretty = JSON.stringify(configJson, undefined, 4);
        document.getElementById('site-config').value = pretty;
      }
    }
  }
}

async function postConfig() {
  const config = document.getElementById('configTextArea').value;
  const organizationName = document.getElementById('org').value;
  const siteName = document.getElementById('site').value;
  const configJson = JSON.parse(config);
  if (siteName === '' || organizationName === '') return;
  const postConfigUrl = `https://admin.hlx.page/config/${organizationName}/sites/${siteName}.json`;
  const options = {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    credentials: 'include',
    body: configJson,
  };
  const resp = await fetch(postConfigUrl, options);
  if (resp.status === 401) {
    const configSubmit = document.getElementById('configSubmit');
    addErrorMessage(`401 Unauthorized. Please login to <a href="https://admin.hlx.page/login">https://admin.hlx.page/login</a> before saving configurations.
      You also need to have a role of admin to save site configurations`, configSubmit);
    return;
  }
  if (resp) {
    const response = await resp.json();
    console.log(JSON.stringify(response));
  }
}

export default function decorate(block) {
  block.innerHTML = `
  <div class="form">
    <form onsubmit="event.preventDefault();">
    <label for="org">Organization Name: </label>
    <input 
      id="org"
      name="org"
      type="text"
      required="true"
      autocomplete="on" 
      placeholder="org-name" 
      title="Please enter the name of the organization where you want to save this configuration to"
    />
    <label for="site">Site Name: </label>
    <input 
      id="site"
      name="site"
      type="text"
      required="true"
      autocomplete="on" 
      placeholder="site-name" 
      title="Please enter the name of the site you want to save this configuration to"
    />
    <button type="submit" id="repoSubmit" name="repoSubmit">Submit</button>
    </form>
  </div>
  <div class="config hidden">
    <form onsubmit="event.preventDefault();">
      <h4 for="org">Configuration: </h4>
      <textarea 
        id="site-config"
        name="site-config"
        required="true"
        readonly="true"
        rows="30"
        cols="100"></textarea>
      <img src="/tools/admin/edit-icon.svg" alt="Edit Icon"></img>
      <button type="submit" id="configSubmit" name="configSubmit" class="hidden">Save</button>
    </form>
  </div>
  `;
  const repoSubmit = block.getElementById('repoSubmit');
  const configSubmit = block.getElementById('configSubmit');
  repoSubmit.addEventListener('click', getConfig);
  configSubmit.addEventListener('click', postConfig);
}
