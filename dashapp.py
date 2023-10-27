import dash
from dash import html,dcc
from dash.dependencies import Input, Output
import pandas as pd
from sqlalchemy import create_engine
import pymysql

pymysql.install_as_MySQLdb()


# Connect to the Sakila database

engine = create_engine('mysql://Jas:Jas098123@localhost/sakila')

query1 = """
select actor.first_name , actor.last_name, count(film_actor.film_id) as film_count
from actor
join film_actor on actor.actor_id = film_actor.actor_id
group by actor.actor_id
having film_count > 15
order by film_count desc;
"""

query2 = """
select customer_id, count(*) as num_rentals
from rental
group by customer_id
order by num_rentals desc
limit 5;
"""

query3 = """
select f.film_id, f.title, sum(p.amount) as total_revenue
from film f
join inventory i on f.film_id = i.film_id
join rental r on i.inventory_id = r.inventory_id
join payment p on r.rental_id = p.rental_id
group by f.title, f.film_id;
"""

query4 = """
select actor.actor_id, count(film_actor.film_id) as film_count
from actor
join film_actor on actor.actor_id = film_actor.actor_id
group by actor.actor_id
having film_count >= 20
order by film_count desc;
"""

query5 = """
SELECT DATE(rental_date) AS rental_day, COUNT(rental_id) AS rental_count
FROM rental, inventory, film, film_category
WHERE rental.inventory_id = inventory.inventory_id AND
      inventory.film_id = film.film_id AND
      film.film_id = film_category.film_id AND
      category_id = 1
GROUP BY DATE(rental_date);

"""

# Create the Dash app
app = dash.Dash(__name__)

# Define the layout of the app
app.layout = html.Div([
    html.H1("Sakila Rental Data Over Time"),
    
    # Dropdown to select a category
    dcc.Dropdown(
        id='category-dropdown',
        options=[
            {'label': 'Category 1', 'value': 1},
            {'label': 'Category 2', 'value': 2},
            # Add more options based on your data
        ],
        value=1  # Default selected option
    ),
    
    # Line chart to display data over time
    dcc.Graph(id='line-chart')
])

# Define callback to update the line chart based on the selected category
@app.callback(
    Output('line-chart', 'figure'),
    [Input('category-dropdown', 'value')]
)
def update_line_chart(selected_category):
    # SQL query to retrieve data for the selected category over time
    query = f"""
    SELECT DATE(rental_date) AS rental_day, COUNT(rental_id) AS rental_count
    FROM rental, inventory, film, film_category
    WHERE rental.inventory_id = inventory.inventory_id AND
    inventory.film_id = film.film_id AND
    film.film_id = film_category.film_id AND
    category_id = {selected_category}
    GROUP BY rental_day;
    """

    rental_data = pd.read_sql(query, engine)

    # Create the line chart
    fig = {
        'data': [
            {
                'x': rental_data['rental_day'],
                'y': rental_data['rental_count'],
                'type': 'bar',
                'marker': {'color': 'blue'}
            }
        ],
        'layout': {
            'title': f'Rental Count for Category {selected_category}',
            'xaxis': {'title': 'Rental Day'},
            'yaxis': {'title': 'Rental Count'}
        }
    }

    return fig

if __name__ == '__main__':
    app.run_server(debug=True)
