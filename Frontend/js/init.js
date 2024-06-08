let userID = [];
let movies = [];
let genreNames = [];
let neoViz;

initPage();

function initPage() {
  addChangePageEvent();
  setOptions();
  initGraph();
}

function addChangePageEvent() {
  let links = document.getElementsByClassName("toolbar-item");
  const homePage = document.getElementById("homePage");
  homePage.classList.add("active");
  Array.from(links).forEach((link) => {
    link.addEventListener("click", (e) => {
      Array.from(links).forEach(
        (link) => (link.style = "background-color:none; ")
      );
      e.target.style = "background-color:#5487df78";
      e.preventDefault();
      const href = link.getAttribute("href");
      const target = document.querySelector(href);
      const current = document.querySelector(".active");
      if (current) {
        current.style.display = "none";

        current.classList.remove("active");
      }
      target.classList.add("active");
    });
  });
}


async function getUserIDs() {
  const query = "MATCH (u:User) RETURN u.userId AS userId ORDER BY userId";
  let result = await runCypherQuery(query);
  result = result.map((el) => el.userId);
  result = result.map((el) => el.low);
  return result;
}

async function getMovies() {
  const query = "MATCH (m:Movie) RETURN m.movieId as id, m.title AS title";
  let result = await runCypherQuery(query);
  result = result.map((el) => {return {id:el.id.low, title:el.title}});
  return result;
}

async function getGenreNames() {
  const query = "MATCH (g:Genre) RETURN g.name AS name";
  let result = await runCypherQuery(query);
  result = result.map((el) => el.name);
  return result;
}


async function setOptions() {
  let select = document.getElementById("functSelect");

  Object.keys(functionalityDescr).forEach((funct) => {
    let option = document.createElement("option");

    option.value = funct;
    option.innerHTML = functionalityDescr[funct];
    select.appendChild(option);
  });
  let option = document.createElement("option");
  option.value ="customQuery"
  option.innerHTML = "Write your own Cypher query that you want to execute"
  select.appendChild(option);
  userID = await getUserIDs();
  movies = await getMovies();
  genreNames = await getGenreNames();

  let selectUser = document.getElementById("userIdSelect");
  userID.forEach((id) => {
    let option = document.createElement("option");
    option.value = id;
    option.innerHTML = id;
    selectUser.appendChild(option);
  });

  let selectMovie = document.getElementById("movieTitelSelect");

  movies.forEach((movie) => {
    let option = document.createElement("option");
    option.value = movie.title;
    option.innerHTML = movie.title;
    selectMovie.appendChild(option);
  });

  let selectGenre = document.getElementById("genreSelect");

  genreNames.forEach((genre) => {
    let option = document.createElement("option");
    option.value = genre;
    option.innerHTML = genre;
    selectGenre.appendChild(option);
  });
}


async function initGraph() {
  neoViz = new NeoVis.default(graphConfig);
  neoViz.render();
}
