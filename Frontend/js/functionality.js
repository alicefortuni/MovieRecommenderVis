
let cypherQuery;
let cypherQuery2;

function executeCustomQuery(){
    closeAllItems();
    showCustomQueryItem();
  
    document.getElementById("form_button").addEventListener("click", async function (event) {
      event.preventDefault();
      cypherQuery = document.getElementById("customQuery").value;
      let res=await runCypherQuery(cypherQuery);
      if(res!=false){
        writeResults(res);
        updateGraph(cypherQuery);
      }
      });
  }
  
  function getMostRatedMovies() {
    closeAllItems();
    document.getElementById("form_button").addEventListener("click", async function (event) {
        event.preventDefault();
  
        cypherQuery =
          "MATCH (m:Movie)<-[:RATED]-(u:User) RETURN m.title AS movie, COUNT(u) AS ratingsCount ORDER BY ratingsCount DESC LIMIT 10";
       
          let res=await runCypherQuery(cypherQuery);
          hideVizDiv() 
          writeResults(res)
        });
     
  }
  
  function getMostRatedUser() {
    closeAllItems();
  
    document.getElementById("form_button").addEventListener("click", async function (event) {
        event.preventDefault();
  
        cypherQuery =
          "MATCH (u:User)-[:RATED]->(m:Movie) RETURN u.userId AS userId, COUNT(m) AS ratedMovies ORDER BY ratedMovies  DESC LIMIT 10";
          let res=await runCypherQuery(cypherQuery);
          hideVizDiv() 
          writeResults(res);
      });
  }
  
  function getMovieRatedByUser() {
    closeAllItems();
    showUserIDItem();
  
    document.getElementById("form_button").addEventListener("click", async function (event) {
        event.preventDefault();
        let userID = document.getElementById("userIdSelect").value;
        cypherQuery =
          "MATCH p=(u:User {userId: "+
          userID +
          "})-[r:RATED]->(m:Movie) RETURN p";
  
          cypherQuery2 =
          "MATCH p=(u:User {userId: "+
          userID +
          "})-[r:RATED]->(m:Movie) RETURN u.userId as userId, r.rating as rating, m.title as movie ORDER BY r.rating DESC";
  
        updateGraph(cypherQuery);
        let res=await runCypherQuery(cypherQuery2);
        writeResults(res);
      });
  }
  
  function getMovieRecommendedForUser() {
    closeAllItems();
    showUserIDItem();
  
    document.getElementById("form_button").addEventListener("click", async function (event) {
        event.preventDefault();
  
        let userID = document.getElementById("userIdSelect").value;
        cypherQuery =
          "MATCH p=(u:User {userId: " +
          userID +
          "})-[:RECOMMENDED]->(m:Movie) RETURN p";
  
          cypherQuery2 =
          "MATCH p=(u:User {userId: "+
          userID +
          "})-[r:RECOMMENDED]->(m:Movie) RETURN u.userId as userId, r.rating as rating, m.title as movie ORDER BY r.rating DESC";
          updateGraph(cypherQuery);
          let res=await runCypherQuery(cypherQuery2);
          writeResults(res);
      });
  }
  
  function getRatingsForMovie() {
    closeAllItems();
    showMovieTitleItem();
    document.getElementById("form_button").addEventListener("click", async function (event) {
        event.preventDefault();
        let title = document.getElementById("movieTitelSelect").value;
        cypherQuery =
          "MATCH (m:Movie {title: '" +
          title +
          "'})<-[r:RATED]-(u:User)  RETURN m, u, r";
  
          cypherQuery2 =
          "MATCH (m:Movie {title: '" +
          title +
          "'})<-[r:RATED]-(u:User)  RETURN m.title as title, u.userId as userId, r.rating as rating ORDER BY r.rating DESC";
  
          updateGraph(cypherQuery);
          let res=await runCypherQuery(cypherQuery2);
          writeResults(res)
      });
  }
  
  function getMovieRatingsMean() {
    closeAllItems();
    showMovieTitleItem();
    document.getElementById("form_button").addEventListener("click", async function (event) {
        event.preventDefault();
        let title = document.getElementById("movieTitelSelect").value;
        
        cypherQuery =
          "MATCH (m:Movie {title: '" +
          title +
          "'})<-[r:RATED]-(u:User) RETURN m.title AS movie, count(r) as totalRatings,  AVG(r.rating) AS avgRating";
          let res=await runCypherQuery(cypherQuery);
          hideVizDiv()
          writeResults(res)
      });
  }
  
  function getPopularMovieByGenre() {
    closeAllItems();
    showGenreNameItem();
  
    document.getElementById("form_button").addEventListener("click", async function (event) {
        event.preventDefault();
        let genre = document.getElementById("genreSelect").value;
        cypherQuery =
          "MATCH (u:User)-[r:RATED]->(m:Movie)-[b:BELONGS_TO]->(g:Genre {name: '" +
          genre +
          "'}) RETURN m, b, g, COUNT(u) AS ratingsCount ORDER BY ratingsCount DESC LIMIT 10";
        
          cypherQuery2 =
          "MATCH (u:User)-[:RATED]->(m:Movie)-[:BELONGS_TO]->(g:Genre {name: '" +
          genre +
          "'}) RETURN m.title AS movie, COUNT(u) AS ratingsCount ORDER BY ratingsCount DESC LIMIT 10";
        
          updateGraph(cypherQuery);
          let res=await runCypherQuery(cypherQuery2);
          writeResults(res)
      });
  }
  
  function getUserFavoriteGenres() {
    closeAllItems();
    showUserIDItem();
  
    document.getElementById("form_button").addEventListener("click", async function (event) {
        event.preventDefault();
        let userID = document.getElementById("userIdSelect").value;
        cypherQuery =
          "MATCH p=(u:User {userId: " +
          userID +
          "})-[r:RATED]->(:Movie)-[:BELONGS_TO]->(g:Genre)  WITH g.name as genre, AVG(r.rating) as avgRating, count(r) as numRatedMovies RETURN genre, avgRating, numRatedMovies order by avgRating desc";
  
          let res=await runCypherQuery(cypherQuery);
          hideVizDiv()
          writeResults(res)
      });
  }