<script lang="ts">
	let { title = "FICHA" } = $props();
</script>

<header>
	<nav>
		<div class="logo">{title}</div>
		<ul>
			<li><a href="/">Home</a></li>
			<li><a href="/sobre">Sobre</a></li>
		</ul>
	</nav>
</header>

<style>
	header {
		background: #2c3e50;
		color: white;
		padding: 1rem;
	}
	nav {
		display: flex;
		justify-content: space-between;
		align-items: center;
		max-width: 1200px;
		margin: 0 auto;
	}
	.logo {
		font-size: 1.5rem;
		font-weight: bold;
	}
	ul {
		list-style: none;
		display: flex;
		gap: 1rem;
		margin: 0;
		padding: 0;
	}
	a {
		color: white;
		text-decoration: none;
	}
	a:hover {
		text-decoration: underline;
	}
</style>
