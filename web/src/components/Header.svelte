<script lang="ts">
	import { withBase } from '../lib/base';

	let { title = "FICHA" } = $props();
</script>

<header data-cobogo-inverted>
	<nav>
		<div class="logo">{title}</div>
		<ul>
			<li><a href={withBase('/')}>Home</a></li>
			<li><a href={withBase('/sobre')}>Sobre</a></li>
		</ul>
	</nav>
</header>

<style>
	header {
		background: var(--cobogo-text);
		color: var(--cobogo-surface);
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
		color: var(--cobogo-surface);
		text-decoration: none;
	}
	a:hover,
	a:focus-visible {
		text-decoration: underline;
	}
</style>
