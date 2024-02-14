/* eslint-disable no-unused-vars */
/* eslint-disable no-undef */

function showAlert() {
  const alertBox = document.getElementById('customAlert');
  alertBox.style.display = 'block';
  setTimeout(() => {
    alertBox.style.display = 'none';
  }, 4000); // Dismiss after 4 seconds
}

function writeToClipboard(blob) {
  const data = [new ClipboardItem({ [blob.type]: blob })];
  navigator.clipboard.write(data);
}

async function populateTags() {
  // Replace with your JSON endpoint
  const tags = '/tags.json';
  const url = new URL(tags, window.location.origin);
  const resp = await fetch(url.toString());
  const response = await resp.json();
  if (response) {
    const { data } = response;
    const subjects = data.map((item) => item.Subjects);
    subjects.sort();
    const industries = data.map((item) => item.Industries);
    industries.sort();
    const selectSubjects = document.getElementById('dropdown-subjects');
    subjects.forEach((item) => {
      if (item) {
        const option = document.createElement('option');
        option.value = item;
        option.text = item;
        selectSubjects.appendChild(option);
      }
    });
    const selectIndustries = document.getElementById('dropdown-industries');
    industries.forEach((item) => {
      if (item) {
        const option = document.createElement('option');
        option.value = item;
        option.text = item;
        selectIndustries.appendChild(option);
      }
    });
  }
}

function processForm() {
  const publishDate = document.getElementById('publishDate').value;
  const title = document.getElementById('title').value;
  const description = document.querySelector('form div#description .ql-editor').innerHTML;
  const abstract = document.querySelector('form div#abstract .ql-editor').innerHTML;
  const subjectsDropdown = document.getElementById('dropdown-subjects');
  const selectedSubjects = Array
    .from(subjectsDropdown.selectedOptions)
    .map((option) => option.value);
  const subjects = selectedSubjects.join(', ');
  const industriesDropdown = document.getElementById('dropdown-industries');
  const selectedIndustries = Array
    .from(industriesDropdown.selectedOptions)
    .map((option) => option.value);
  const industries = selectedIndustries.join(', ');
  // create the html to paste into the word doc
  const htmlToPaste = `
    <h1>${title}</h1>
    <br>
    ${abstract}
    ---

    <table border="1">
      <tr bgcolor="#f7caac">
        <td colspan="2">Metadata</td>
      </tr>
      <tr>
        <td>Template</td>
        <td>Article</td>
      </tr>
      <tr>
        <td>Published Date</td>
        <td>${publishDate}</td>
      </tr>
      <tr>
        <td>Title</td>
        <td>${title}</td>
      </tr>
      <tr>
        <td>Description</td>
        <td>${description}</td>
      </tr>
      <tr>
        <td>Abstract</td>
        <td>${abstract}</td>
      </tr>
      <tr>
        <td>Subjects</td>
        <td>${subjects}</td>
      </tr>
      <tr>
        <td>Industries</td>
        <td>${industries}</td>
      </tr>
      <tr>
        <td>Keywords</td>
        <td></td>
      </tr>
    </table>
  `;
  writeToClipboard(new Blob([htmlToPaste], { type: 'text/html' }));
  showAlert();
}

async function init() {
  await populateTags();
  const rteOptions = {
    modules: {
      toolbar: [
        ['bold', 'italic', 'underline'],
        ['link'],
        ['clean'],
      ],
    },
    theme: 'snow',
  };
  const descContainer = document.querySelector('form div#description');
  const descriptionEditor = await new Quill(descContainer, rteOptions);
  const abstractContainer = document.querySelector('form div#abstract');
  const abstractEditor = await new Quill(abstractContainer, rteOptions);
  const copyButton = document.getElementById('copyToClipboard');
  copyButton.addEventListener('click', () => {
    processForm();
  });
}

await init();
