async function runCypherQuery(query) {
  const session = driver.session();
  try {
    const result = await session.run(query);
    return result.records.map((record) => record.toObject());
  } catch(error){
    hideVizDiv();
    showResultsDiv();
    document.getElementById("results").innerHTML ="<div style='width:100%;'><br><h3 style=>Error</h3><p>"+error.message+"</p></div>" 
    return false;
  }
    finally {
    await session.close();
  }
}

async function updateGraph(cypherQuery) {
  neoViz.clearNetwork();
  try {
    await neoViz.updateWithCypher(cypherQuery);
    showVizDiv();
  } catch (error) {
    showResultsDiv();
    document.getElementById("results").innerHTML ="<h3>Error</h3><p>"+error.message+"<p>" 
  }

}

function writeResults(results) {  
  document.getElementById("results").innerHTML = "";
  let table=document.createElement("table");
  let tr=document.createElement("tr");
  let keys= Object.keys(results[0])
  keys.forEach(key=>{
    let th=document.createElement("th");
    th.textContent=key;
    tr.appendChild(th)
  })
  table.appendChild(tr)
  results.forEach(row=>{
    let tr=document.createElement("tr");
    keys.forEach(key=>{
      let td=document.createElement("td");
      td.textContent=row[key];
      tr.appendChild(td)
    })
    table.appendChild(tr)
  })

  document.getElementById("results").appendChild(table);
  showResultsDiv();
}

function showVizDiv() {
  document.getElementById("viz").style.display = "block";
}

function showResultsDiv() {
  document.getElementById("results").style.display = "flex";
}

function hideVizDiv() {
  document.getElementById("viz").style.display = "none";
}

function hideResultsDiv() {
  document.getElementById("results").innerHTML = "";
  document.getElementById("results").style.display = "none";
}

function changeFunct(value) {
  switch (value) {
    case "customQuery":
      executeCustomQuery();
      break;
    case "mostRatedMovies":
      getMostRatedMovies();
      break;
    case "mostRatedUser":
      getMostRatedUser();
      break;
    case "userMovieRated":
      getMovieRatedByUser();
      break;
    case "userMovieRecommendend":
      getMovieRecommendedForUser();
      break;
    case "ratingForSameMovie":
      getRatingsForMovie();
      break;
    case "movieMean":
      getMovieRatingsMean();
      break;
    case "popularMoviesByGenre":
      getPopularMovieByGenre();
      break;
    case "userFavoriteGenres":
      getUserFavoriteGenres();
      break;
    default:
      console.error("Error");
  }
}

function closeAllItems() {
  hideVizDiv();
  hideResultsDiv();
  const button = document.getElementById("form_button");
  const newButton = button.cloneNode(true);
  button.parentNode.replaceChild(newButton, button);
  Array.from(document.getElementsByClassName("toggleItem")).forEach(
    (el) => (el.style.display = "none")
  );
}

function showCustomQueryItem() {
  document.getElementById("customQuery_item").style.display = "flex";
}

function showUserIDItem() {
  document.getElementById("userID_item").style.display = "flex";
}

function showMovieTitleItem() {
  document.getElementById("movieTitle_item").style.display = "flex";
}

function showGenreNameItem() {
  document.getElementById("genre_item").style.display = "flex";
}
