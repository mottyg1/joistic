It hides the data modeling and still allows using the elastic dsl.

Lets say we have two indexes: people and cityzip
people:
[{
	"name": "motty",
	"age": 20,
	"city": "jerusalem",
	"zip": 678923
},
{
	"name": "josh",
	"age": 25,
	"zip": 1234
},
{
	"name": "gur",
	"age": 35,
	"city": "holon",
	"zip": 54839
},
{
	"name": "bibi",
	"age": 25,
	"zip": 9876
}]

cityzip:
[{
	"city": "jerusalem",
	"zip": 9876
},
{
	"city": "tel aviv",
	"zip": 1234
}]


If we want to query on city we need to preform a join on the second index:
1) Query what zips are related to a city
2) Incorporate those zips into the query in a should clause

Exapmle:

"query": {
	"term": {
		"city": "jerusalem"
	}
}

Should be turned into:

"query": {
	"bool": {
		"should": [
			{
				"term": {
					"city": "jerusalem"
				}
			},
			{
				"terms": {
					"zip": [9876]
				}
			}
		]
	}
}