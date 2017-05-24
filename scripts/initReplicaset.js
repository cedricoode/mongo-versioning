if (rs.status().ok === 0) {
	rs.initiate({
		_id: 'rs0',
		members: [
			{
				_id: 0,
				host: 'localhost:27018'
			},
			{
				_id: 1,
				host: 'localhost:27019'
			},
			{
				_id: 2,
				host: 'localhost:27020'
			}
		]
	})
}